# prompt: generate a python script to break a .csv file into smaller files for ingestion
 
import pandas as pd
import os
 
def split_csv(input_file, output_dir, chunk_size):
    """
    Splits a large CSV file into smaller files.
 
    Args:
        input_file (str): Path to the input CSV file.
        output_dir (str): Path to the directory where the output files will be saved.
        chunk_size (int): Number of rows per output file.
    """
 
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
 
    for i, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size)):
        output_file = os.path.join(output_dir, f"chunk_{i}.csv")
        chunk.to_csv(output_file, index=False)
        print(f"Created {output_file}")
 
 
# Example usage
input_csv = "/content/3.6M-Japan-lifebear.com-Largest-Notebook-App-UsersDB-csv-2019.csv"  # Replace with your input file
output_directory = "/content/split_csv_files"  # Replace with your desired output directory
rows_per_file = 1000000  # Adjust as needed
 
split_csv(input_csv, output_directory, rows_per_file)
