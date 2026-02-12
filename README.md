bdconvert

Production-grade CLI tool for converting common big-data file formats.

* Supported Formats

CSV

JSON (array)

JSONL (JSON Lines)

Parquet

Avro

ORC

* Features

Streaming-based architecture for large files

Automatic format detection via file extension

Atomic writes (safe output replacement)

Column selection support

Avro schema inference (best-effort)

Snappy compression support (Parquet/ORC)

Batch processing using PyArrow

* Installation
git clone https://github.com/<your-username>/bdconvert.git
cd bdconvert
pip install -r requirements.txt


Or install dependencies manually:

pip install pyarrow fastavro


* Usage

Basic usage:

python bdconvert.py input.csv output.parquet


Force input/output formats:

python bdconvert.py input.data output.avro \
    --input-format json \
    --output-format avro


Convert CSV → Parquet with compressibdconverton:

python bdconvert.py data.csv data.parquet \
    --compression snappy


Select specific columns:

python bdconvert.py data.parquet out.jsonl \
    --columns id,name,age

* Architecture

Row-based streaming for CSV & JSON

Arrow RecordBatch processing for Parquet/ORC

Batch-based Avro writing

Memory-efficient conversion pipeline

Atomic file writing using temporary files

