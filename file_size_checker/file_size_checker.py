import argparse
import os

def get_file_size(file_path):
 if not os.path.exists(file_path):
     print("file not found")
     return

 size_bytes = os.path.getsize(file_path) 

 size_mb = size_bytes / (1024 * 1024)

 print(f"File size: {size_mb:2f} MB")

def main():
   parser = argparse.ArgumentParser(description="File Size Checker Utility")
   parser.add_argument("file" , help="Path to the file")

   args = parser.parse_args()

   get_file_size(args.file)

if __name__ == "__main__":
   main()   


