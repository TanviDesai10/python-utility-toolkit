import argparse
import csv
import json
import os

def csv_to_json(csv_file, json_file):
    data = []

    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)

    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

    print(f" JSON file created: {json_file}")

def main():
    parser = argparse.ArgumentParser(description="CSV to JSON Converter Utility")
    parser.add_argument("csv_file", help="Input CSV file")
    parser.add_argument("json_file", help="Output JSON file")

    args = parser.parse_args()

    if not os.path.exists(args.csv_file):
        print(" CSV file not found")
        return

    csv_to_json(args.csv_file, args.json_file)

if __name__ == "__main__":
    main()
