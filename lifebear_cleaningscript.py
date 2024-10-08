import pandas as pd
import re
import logging
import os

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
expected_columns = ['id', 'login_id', 'mail_address', 'password', 'created_at', 'salt', 'birthday_on', 'gender']
rename_mapping = {
    'ID': 'id',
    'Name': 'login_id',
    'Email': 'mail_address',
    'Date_of_Birth': 'birthday_on',
    'Salary': 'password'  # Adjust this if necessary
}
chunk_size = 100000
columns_to_clean = ['login_id', 'mail_address']
pattern = r'[^\w\s@.\-]'

# Function to validate email
def is_valid_email(email):
    """Validates the format of a mail address."""
    if pd.isna(email):  # Check if the email is NaN or NA
        return False
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return bool(re.match(pattern, email))

# Function to validate login_id (alphanumeric with underscores or dashes)
def is_valid_login_id(login_id):
    """Validates the format of a login ID."""
    pattern = r'^[\w\-]+$'
    return bool(re.match(pattern, login_id))

# Function to validate password (minimum 8 characters, includes letters and numbers)
def is_valid_password(password):
    """Validates the security of a password."""
    return len(password) >= 8 and any(c.isalpha() for c in password) and any(c.isdigit() for c in password)

# Function to validate date (e.g., birthday_on or created_at)
def is_valid_date(date):
    """Validates if a date is in a valid range."""
    return pd.notna(date) and pd.Timestamp.now() > date

# Function to validate gender (0 for Female, 1 for Male)
def is_valid_gender(gender):
    """Validates the gender field, allowing only 0 (female) and 1 (male)."""
    return gender in [0, 1]

# Function to handle errors during data processing
def safe_process(func, *args, **kwargs):
    """Wraps a function in a try-except block for error handling."""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return pd.NA

def process_data(file_path, clean_output_file, garbage_output_file, duplicates_output_file):
    """Processes the dataset in chunks and handles cleaning, validation, and error logging."""
    logging.info(f"Starting data processing for {file_path} with chunk size {chunk_size}")
    
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")
    
    all_data = pd.DataFrame()  # Create an empty DataFrame to concatenate clean chunks
    all_garbage = pd.DataFrame()  # Create an empty DataFrame for garbage rows
    
    # Use low_memory=False to avoid DtypeWarning
    for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size, delimiter=';', low_memory=False)):
        logging.info(f"Processing chunk {i+1}")
        
        try:
            # Make a copy to avoid the "Setting on a copy of a slice" warning
            chunk = chunk.copy()

            # Print the column names for debugging
            logging.info(f"Columns in chunk {i+1}: {chunk.columns.tolist()}")
            
            # Rename columns
            chunk.rename(columns=rename_mapping, inplace=True)
            
            # Ensure expected columns are present
            for col in expected_columns:
                if col not in chunk.columns:
                    chunk[col] = pd.NA  # or a default value
            
            # Reorder columns to match the required order
            chunk = chunk[expected_columns]
            
            # Remove empty lines
            chunk.dropna(how='all', inplace=True)

            # Drop rows with invalid or missing email addresses
            chunk = chunk[chunk['mail_address'].notna()]  # Keep rows with valid email addresses only
            chunk['Valid_Email'] = chunk['mail_address'].apply(is_valid_email)
            chunk = chunk[chunk['Valid_Email'] == True]  # Keep rows where email is valid

            # Remove unauthorized characters from specified columns
            for col in columns_to_clean:
                chunk[col] = safe_process(lambda: chunk[col].astype(str).str.replace(pattern, '', regex=True))
            
            # Standardize data types
            chunk['birthday_on'] = pd.to_datetime(chunk['birthday_on'], errors='coerce')
            chunk['created_at'] = pd.to_datetime(chunk['created_at'], errors='coerce')
            
            # Validate gender field, and replace any 'false'/'true' values with 0 and 1
            chunk['Valid_Gender'] = chunk['gender'].apply(is_valid_gender)
            chunk.loc[chunk['gender'] == 'false', 'gender'] = 0  # Female (replace 'false' with 0)
            chunk.loc[chunk['gender'] == 'true', 'gender'] = 1   # Male (replace 'true' with 1)

            # Convert gender to 0/1 for both clean and garbage data
            chunk.loc[chunk['gender'] == 'false', 'gender'] = 0  # Female
            chunk.loc[chunk['gender'] == 'true', 'gender'] = 1   # Male
            
            # Validate birthday and gender
            chunk['Valid_Birthday'] = chunk['birthday_on'].apply(is_valid_date)
            
            # Ensure both validation columns are of boolean type before applying '&' operation
            chunk['Valid_Birthday'] = chunk['Valid_Birthday'].astype(bool)
            chunk['Valid_Gender'] = chunk['Valid_Gender'].astype(bool)
            
            # Separate clean and garbage data
            valid_chunk = chunk[chunk['Valid_Birthday'] & chunk['Valid_Gender']].drop(columns=['Valid_Birthday', 'Valid_Gender', 'Valid_Email'])
            garbage_chunk = chunk[~(chunk['Valid_Birthday'] & chunk['Valid_Gender'])]

            # Append the garbage rows to the garbage DataFrame, including 'salt'
            all_garbage = pd.concat([all_garbage, garbage_chunk], ignore_index=True)

            # Append valid data to the all_data DataFrame
            all_data = pd.concat([all_data, valid_chunk], ignore_index=True)
        
        except Exception as e:
            logging.error(f"Error processing chunk {i+1}: {e}")
    
    logging.info("Finished processing all chunks.")
    
    # Write the garbage data to garbage output file with '0'/'1' for gender and correct column order
    if not all_garbage.empty:
        all_garbage[expected_columns].to_csv(garbage_output_file, index=False)
        logging.info(f"Garbage data exported to {garbage_output_file}")
    
    # Write valid data to clean output file with '0'/'1' for gender and correct column order
    if not all_data.empty:
        all_data[expected_columns].to_csv(clean_output_file, index=False)
        logging.info(f"Clean data exported to {clean_output_file}")
    
    # Step 4: Identify and export duplicates
    duplicates = all_data[all_data.duplicated(keep=False)]
    
    if not duplicates.empty:
        duplicates.to_csv(duplicates_output_file, index=False)
        logging.info(f"Duplicates exported to {duplicates_output_file}")
    else:
        logging.info("No duplicates found.")

# Main entry point
if __name__ == "__main__":
    # File paths
    input_file = r'C:\Users\garne\Documents\DATA CLEANING\DataSets\3.6M-Japan-lifebear.com-Largest-Notebook-App-UsersDB-csv-2019.csv'  # Replace with your input file
    clean_output_file = r'C:\Users\garne\Documents\DATA CLEANING\Clean Sets\Clean_Japan_test.csv'
    garbage_output_file = r'C:\Users\garne\Documents\DATA CLEANING\Garbage Sets\Garbage_Japan_test.csv'
    duplicates_output_file = r'C:\Users\garne\Documents\DATA CLEANING\Duplicate Sets\Duplicates_Japan_test.csv'  # Path for duplicate records
    
    try:
        # Process the data and save outputs to the respective files
        process_data(input_file, clean_output_file, garbage_output_file, duplicates_output_file)
        
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
        print(f"An error occurred: {e}")
