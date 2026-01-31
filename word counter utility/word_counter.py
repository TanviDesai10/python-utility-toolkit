import argparse
import os

def count_word(file_path):
    if not os.path.exists(file_path):
        print("File not found")
        return 0

    with open(file_path, "r") as f:
        text = f.read()

    words = text.split()
    return len(words)

def main():
    parser = argparse.ArgumentParser(description="Word Counter Utility")
    parser.add_argument("file", help="Input text file")

    args = parser.parse_args()

    total_words = count_word(args.file)
    print(f"Total words: {total_words}")

if __name__ == "__main__":
    main()
