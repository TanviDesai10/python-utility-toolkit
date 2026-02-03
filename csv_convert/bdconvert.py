#!/usr/bin/env python3
"""
bdconvert.py - Production-grade CLI for converting common big-data file formats.

Supported formats:
  - CSV
  - JSON (array) / JSONL (json lines)
  - Parquet
  - Avro
  - ORC

Notes:
  - Parquet/ORC require pyarrow
  - Avro requires fastavro
  - JSON array (single huge list) is NOT streaming-friendly; JSONL is recommended
"""

from _future_ import annotations

import argparse
import csv as pycsv
import io
import json
import logging
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

LOG = logging.getLogger("bdconvert")


# -------------------------
# Dependency helpers
# -------------------------
def require(module_name: str, install_hint: str) -> None:
    try:
        _import_(module_name)
    except ImportError as e:
        raise SystemExit(
            f"Missing dependency '{module_name}'. Install with:\n  {install_hint}"
        ) from e


def _try_import_pyarrow():
    try:
        import pyarrow as pa  # noqa
        return True
    except ImportError:
        return False


def _try_import_fastavro():
    try:
        import fastavro  # noqa
        return True
    except ImportError:
        return False


# -------------------------
# Formats
# -------------------------
FMT_CSV = "csv"
FMT_JSON = "json"     # JSON array OR JSON object per file (not jsonl)
FMT_JSONL = "jsonl"   # JSON Lines
FMT_PARQUET = "parquet"
FMT_AVRO = "avro"
FMT_ORC = "orc"

ALL_FORMATS = (FMT_CSV, FMT_JSON, FMT_JSONL, FMT_PARQUET, FMT_AVRO, FMT_ORC)

EXT_TO_FMT = {
    ".csv": FMT_CSV,
    ".tsv": FMT_CSV,
    ".json": FMT_JSON,
    ".jsonl": FMT_JSONL,
    ".ndjson": FMT_JSONL,
    ".parquet": FMT_PARQUET,
    ".pq": FMT_PARQUET,
    ".avro": FMT_AVRO,
    ".orc": FMT_ORC,
}


def detect_format(path: Path, forced: Optional[str]) -> str:
    if forced:
        forced = forced.lower().strip()
        if forced not in ALL_FORMATS:
            raise SystemExit(f"Unsupported --input-format '{forced}'. Use one of: {', '.join(ALL_FORMATS)}")
        return forced

    ext = path.suffix.lower()
    if ext in EXT_TO_FMT:
        return EXT_TO_FMT[ext]

    raise SystemExit(
        f"Could not detect input format from extension '{ext}'. "
        f"Please provide --input-format ({', '.join(ALL_FORMATS)})."
    )


def detect_output_format(path: Path, forced: Optional[str]) -> str:
    if forced:
        forced = forced.lower().strip()
        if forced not in ALL_FORMATS:
            raise SystemExit(f"Unsupported --output-format '{forced}'. Use one of: {', '.join(ALL_FORMATS)}")
        return forced

    ext = path.suffix.lower()
    if ext in EXT_TO_FMT:
        return EXT_TO_FMT[ext]

    raise SystemExit(
        f"Could not detect output format from extension '{ext}'. "
        f"Please provide --output-format ({', '.join(ALL_FORMATS)})."
    )


# -------------------------
# Options
# -------------------------
@dataclass(frozen=True)
class ConvertOptions:
    batch_size: int = 65536

    # CSV
    delimiter: str = ","
    encoding: str = "utf-8"
    csv_has_header: bool = True
    csv_fieldnames: Optional[List[str]] = None

    # JSON
    json_lines: bool = True  # default output is JSONL
    json_indent: Optional[int] = None

    # Parquet/ORC
    compression: Optional[str] = "snappy"

    # General
    columns: Optional[List[str]] = None


# -------------------------
# Atomic output (safe write)
# -------------------------
class AtomicWriter:
    def _init_(self, target_path: Path):
        self.target_path = target_path
        self.tmp_path: Optional[Path] = None

    def _enter_(self) -> Path:
        self.target_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=f".{self.target_path.name}.",
            suffix=".tmp",
            dir=str(self.target_path.parent),
        )
        os.close(fd)
        self.tmp_path = Path(tmp)
        return self.tmp_path

    def _exit_(self, exc_type, exc, tb) -> None:
        if self.tmp_path is None:
            return
        if exc is None:
            os.replace(self.tmp_path, self.target_path)
        else:
            try:
                self.tmp_path.unlink(missing_ok=True)
            except Exception:
                pass


# -------------------------
# Reading -> streaming rows (dict)
# -------------------------
def iter_rows_csv(path: Path, opts: ConvertOptions) -> Iterator[Dict[str, object]]:
    with path.open("r", encoding=opts.encoding, newline="") as f:
        if opts.csv_has_header:
            reader = pycsv.DictReader(f, delimiter=opts.delimiter)
        else:
            if not opts.csv_fieldnames:
                raise SystemExit("CSV without header requires --fieldnames col1,col2,...")
            reader = pycsv.DictReader(f, delimiter=opts.delimiter, fieldnames=opts.csv_fieldnames)

        for row in reader:
            yield row


def _looks_like_json_lines(path: Path, encoding: str) -> bool:
    # Heuristic: first non-empty line starts with { or [
    # If first non-empty line is { and file has many lines, likely jsonl.
    try:
        with path.open("r", encoding=encoding, errors="replace") as f:
            for _ in range(50):
                line = f.readline()
                if not line:
                    break
                s = line.strip()
                if not s:
                    continue
                if s.startswith("{"):
                    return True
                if s.startswith("["):
                    return False
                # if weird, keep scanning
    except Exception:
        return False
    return False


def iter_rows_json(path: Path, opts: ConvertOptions, input_is_jsonl: bool) -> Iterator[Dict[str, object]]:
    if input_is_jsonl:
        with path.open("r", encoding=opts.encoding, errors="strict") as f:
            for lineno, line in enumerate(f, start=1):
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except json.JSONDecodeError as e:
                    raise SystemExit(f"Invalid JSONL at line {lineno}: {e}") from e
                if not isinstance(obj, dict):
                    # allow non-dict records, but normalize
                    yield {"value": obj}
                else:
                    yield obj
        return

    # JSON array/object (not streaming friendly)
    LOG.warning("Input appears to be JSON (non-JSONL). Loading entire file into memory. Prefer JSONL for large files.")
    with path.open("r", encoding=opts.encoding) as f:
        data = json.load(f)

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
            else:
                yield {"value": item}
    elif isinstance(data, dict):
        # single object: emit once
        yield data
    else:
        yield {"value": data}


# -------------------------
# Arrow helpers (for Parquet/ORC efficiency)
# -------------------------
def iter_recordbatches_from_rows(rows: Iterable[Dict[str, object]], batch_size: int):
    """Convert python dict-rows into pyarrow RecordBatches in chunks."""
    require("pyarrow", "pip install pyarrow")
    import pyarrow as pa

    buf: List[Dict[str, object]] = []
    for r in rows:
        buf.append(r)
        if len(buf) >= batch_size:
            table = pa.Table.from_pylist(buf)
            for batch in table.to_batches(max_chunksize=batch_size):
                yield batch
            buf.clear()

    if buf:
        table = pa.Table.from_pylist(buf)
        for batch in table.to_batches(max_chunksize=batch_size):
            yield batch


def iter_recordbatches_from_parquet(path: Path, batch_size: int, columns: Optional[List[str]]):
    require("pyarrow", "pip install pyarrow")
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(str(path))
    for batch in pf.iter_batches(batch_size=batch_size, columns=columns):
        yield batch


def read_orc_as_table(path: Path, columns: Optional[List[str]]):
    require("pyarrow", "pip install pyarrow")
    import pyarrow.orc as orc

    f = orc.ORCFile(str(path))
    table = f.read(columns=columns)
    return table


def iter_recordbatches_from_avro(path: Path, batch_size: int):
    require("fastavro", "pip install fastavro")
    require("pyarrow", "pip install pyarrow")
    import fastavro
    import pyarrow as pa

    with path.open("rb") as fo:
        reader = fastavro.reader(fo)
        buf: List[Dict[str, object]] = []
        for rec in reader:
            if not isinstance(rec, dict):
                rec = {"value": rec}
            buf.append(rec)
            if len(buf) >= batch_size:
                table = pa.Table.from_pylist(buf)
                for b in table.to_batches(max_chunksize=batch_size):
                    yield b
                buf.clear()

        if buf:
            table = pa.Table.from_pylist(buf)
            for b in table.to_batches(max_chunksize=batch_size):
                yield b


# -------------------------
# Writers
# -------------------------
def write_json(rows: Iterable[Dict[str, object]], out_path: Path, json_lines: bool, indent: Optional[int]) -> None:
    with AtomicWriter(out_path) as tmp:
        if json_lines:
            with tmp.open("w", encoding="utf-8") as f:
                for obj in rows:
                    f.write(json.dumps(obj, ensure_ascii=False))
                    f.write("\n")
        else:
            # JSON array
            with tmp.open("w", encoding="utf-8") as f:
                f.write("[\n" if indent else "[")
                first = True
                for obj in rows:
                    if not first:
                        f.write(",\n" if indent else ",")
                    first = False
                    f.write(json.dumps(obj, ensure_ascii=False, indent=indent))
                f.write("\n]\n" if indent else "]")


def write_csv(rows: Iterable[Dict[str, object]], out_path: Path, delimiter: str) -> None:
    with AtomicWriter(out_path) as tmp:
        with tmp.open("w", encoding="utf-8", newline="") as f:
            writer = None
            for row in rows:
                if writer is None:
                    fieldnames = list(row.keys())
                    writer = pycsv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter, extrasaction="ignore")
                    writer.writeheader()
                writer.writerow({k: ("" if v is None else v) for k, v in row.items()})


def write_parquet_from_batches(batches: Iterable["object"], out_path: Path, compression: Optional[str]) -> None:
    require("pyarrow", "pip install pyarrow")
    import pyarrow as pa
    import pyarrow.parquet as pq

    with AtomicWriter(out_path) as tmp:
        writer: Optional[pq.ParquetWriter] = None
        try:
            for batch in batches:
                if not isinstance(batch, pa.RecordBatch):
                    raise SystemExit("Internal error: expected pyarrow.RecordBatch")
                table = pa.Table.from_batches([batch])
                if writer is None:
                    writer = pq.ParquetWriter(str(tmp), table.schema, compression=compression)
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()


def write_orc_from_table(table: "object", out_path: Path, compression: Optional[str]) -> None:
    """
    ORC writing in pyarrow is table-based (not ideal for massive files).
    For very large datasets, prefer Parquet output or write ORC per-partition upstream.
    """
    require("pyarrow", "pip install pyarrow")
    import pyarrow.orc as orc

    with AtomicWriter(out_path) as tmp:
        # pyarrow.orc.write_table supports a 'compression' argument in most modern versions
        try:
            orc.write_table(table, str(tmp), compression=compression)
        except TypeError:
            # if older pyarrow doesn't accept compression param here
            orc.write_table(table, str(tmp))


def _infer_avro_schema_from_sample(records: List[Dict[str, object]], name: str = "record") -> Dict[str, object]:
    """
    Basic Avro schema inference (best-effort).
    Complex nested structures may require a user-provided schema.
    """
    def avro_type(v):
        if v is None:
            return "null"
        if isinstance(v, bool):
            return "boolean"
        if isinstance(v, int) and not isinstance(v, bool):
            return "long"
        if isinstance(v, float):
            return "double"
        if isinstance(v, (bytes, bytearray)):
            return "bytes"
        if isinstance(v, str):
            return "string"
        # fallback: stringify
        return "string"

    keys = set()
    for r in records:
        keys.update(r.keys())

    fields = []
    for k in sorted(keys):
        types = set()
        for r in records:
            types.add(avro_type(r.get(k)))
        if "null" in types and len(types) > 1:
            # union with null first
            ordered = ["null"] + [t for t in sorted(types) if t != "null"]
            ftype: Union[str, List[str]] = ordered
        elif len(types) == 1:
            ftype = next(iter(types))
        else:
            ftype = ["null", "string"]
        fields.append({"name": k, "type": ftype})

    return {"type": "record", "name": name, "fields": fields}


def write_avro(rows: Iterable[Dict[str, object]], out_path: Path, schema_path: Optional[Path], batch_size: int) -> None:
    require("fastavro", "pip install fastavro")
    import fastavro

    schema = None
    if schema_path:
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

    with AtomicWriter(out_path) as tmp:
        with tmp.open("wb") as fo:
            buf: List[Dict[str, object]] = []
            writer_initialized = False

            for r in rows:
                buf.append(r)
                if len(buf) >= batch_size:
                    if not writer_initialized:
                        if schema is None:
                            schema = _infer_avro_schema_from_sample(buf, name="bdconvert_record")
                        schema = fastavro.parse_schema(schema)
                        fastavro.writer(fo, schema, buf)
                        writer_initialized = True
                    else:
                        # append blocks
                        fastavro.writer(fo, schema, buf)
                    buf.clear()

            if buf:
                if not writer_initialized:
                    if schema is None:
                        schema = _infer_avro_schema_from_sample(buf, name="bdconvert_record")
                    schema = fastavro.parse_schema(schema)
                fastavro.writer(fo, schema, buf)


# -------------------------
# Convert pipeline
# -------------------------
def rows_from_input(path: Path, in_fmt: str, opts: ConvertOptions) -> Iterable[Dict[str, object]]:
    if in_fmt == FMT_CSV:
        return iter_rows_csv(path, opts)

    if in_fmt in (FMT_JSON, FMT_JSONL):
        # If user forced json/jsonl, follow it. If json was auto-detected, try to guess JSONL.
        is_jsonl = (in_fmt == FMT_JSONL)
        if in_fmt == FMT_JSON:
            is_jsonl = _looks_like_json_lines(path, opts.encoding)
        return iter_rows_json(path, opts, input_is_jsonl=is_jsonl)

    if in_fmt == FMT_PARQUET:
        # We'll convert parquet -> rows via pyarrow batches downstream as needed
        raise SystemExit("Internal: parquet rows should be handled via arrow batches, not rows().")

    if in_fmt == FMT_AVRO:
        raise SystemExit("Internal: avro rows should be handled via arrow batches, not rows().")

    if in_fmt == FMT_ORC:
        raise SystemExit("Internal: ORC is handled as table, not rows().")

    raise SystemExit(f"Unsupported input format: {in_fmt}")


def convert_file(in_path: Path, out_path: Path, in_fmt: str, out_fmt: str, opts: ConvertOptions, avro_schema: Optional[Path]) -> None:
    LOG.info("Converting %s (%s) -> %s (%s)", in_path, in_fmt, out_path, out_fmt)

    # Case 1: Output needs arrow-efficient formats
    if out_fmt in (FMT_PARQUET, FMT_ORC):
        require("pyarrow", "pip install pyarrow")

        import pyarrow as pa  # noqa

        if in_fmt == FMT_PARQUET:
            batches = iter_recordbatches_from_parquet(in_path, opts.batch_size, opts.columns)
        elif in_fmt == FMT_AVRO:
            batches = iter_recordbatches_from_avro(in_path, opts.batch_size)
        elif in_fmt == FMT_ORC:
            table = read_orc_as_table(in_path, opts.columns)
            batches = iter(table.to_batches(max_chunksize=opts.batch_size))
        else:
            # CSV/JSON/JSONL -> rows -> batches
            rows = rows_from_input(in_path, in_fmt, opts)
            batches = iter_recordbatches_from_rows(rows, opts.batch_size)

        if out_fmt == FMT_PARQUET:
            write_parquet_from_batches(batches, out_path, compression=opts.compression)
            return

        if out_fmt == FMT_ORC:
            # ORC writing: easiest via table (not perfect for huge files)
            require("pyarrow", "pip install pyarrow")
            import pyarrow as pa

            # materialize batches into a table
            table = pa.Table.from_batches(list(batches))
            write_orc_from_table(table, out_path, compression=opts.compression)
            return

    # Case 2: Output is row-oriented (CSV / JSON / JSONL / AVRO)
    # Build row iterator from any input
    if in_fmt == FMT_PARQUET:
        require("pyarrow", "pip install pyarrow")
        rows_iter = _rows_from_arrow_batches(iter_recordbatches_from_parquet(in_path, opts.batch_size, opts.columns))
    elif in_fmt == FMT_AVRO:
        rows_iter = _rows_from_arrow_batches(iter_recordbatches_from_avro(in_path, opts.batch_size))
    elif in_fmt == FMT_ORC:
        table = read_orc_as_table(in_path, opts.columns)
        rows_iter = _rows_from_arrow_table(table, batch_size=opts.batch_size)
    else:
        rows_iter = rows_from_input(in_path, in_fmt, opts)

    if opts.columns:
        rows_iter = _select_columns(rows_iter, opts.columns)

    if out_fmt == FMT_CSV:
        write_csv(rows_iter, out_path, delimiter=opts.delimiter)
        return

    if out_fmt in (FMT_JSON, FMT_JSONL):
        # For output: default to JSONL unless user asked json array
        json_lines = opts.json_lines or (out_fmt == FMT_JSONL)
        write_json(rows_iter, out_path, json_lines=json_lines, indent=opts.json_indent)
        return

    if out_fmt == FMT_AVRO:
        write_avro(rows_iter, out_path, schema_path=avro_schema, batch_size=opts.batch_size)
        return

    raise SystemExit(f"Unsupported output format: {out_fmt}")


def _rows_from_arrow_batches(batches: Iterable["object"]) -> Iterator[Dict[str, object]]:
    require("pyarrow", "pip install pyarrow")
    import pyarrow as pa

    for b in batches:
        if not isinstance(b, pa.RecordBatch):
            raise SystemExit("Internal error: expected RecordBatch.")
        # batch.to_pylist() returns List[Dict[str, object]]
        for row in b.to_pylist():
            yield row


def _rows_from_arrow_table(table: "object", batch_size: int) -> Iterator[Dict[str, object]]:
    require("pyarrow", "pip install pyarrow")
    for b in table.to_batches(max_chunksize=batch_size):
        for row in b.to_pylist():
            y