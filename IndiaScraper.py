import requests
import pandas as pd
from datetime import datetime, timezone
import re
import time

# Helper function to remove special characters from strings
def clean_text(text):
    if isinstance(text, str):
        return re.sub(r'[^a-zA-Z0-9\s]', '', text)  # Retain only alphanumeric characters and spaces
    return text

# Step 1: Extract - Function to Extract Data from API
def extract_data_from_api(url, headers, querystring, pages=5):
    all_jobs = []

    # Loop through the pages
    for page in range(1, pages + 1):
        querystring["page"] = str(page)  # Update the page number
        try:
            response = requests.get(url, headers=headers, params=querystring)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                data = response.json()  # Return the parsed JSON response
                if 'data' in data:
                    all_jobs.extend(data['data'])
            else:
                print(f"Failed to retrieve data from page {page}: {response.status_code}")
                print(f"Response content: {response.content}")  # Log the response content for further debugging
                time.sleep(5)  # Add a longer delay between requests
        except Exception as e:
            print(f"Error while retrieving data from page {page}: {e}")
            time.sleep(5)  # Ensure there's a pause before continuing
    
    return all_jobs

# Step 2: Transform - Helper Function to Extract Skills and Qualifications
def extract_skills_and_qualifications(job_data):
    potential_keys = ['job_required_skills', 'skills', 'certifications', 'qualifications', 'job_description']
    collected_skills = set()  # Use a set to avoid duplicates

    for key in potential_keys:
        content = job_data.get(key)
        if content:
            if isinstance(content, list):
                collected_skills.update(content)  # Add items from the list
            elif isinstance(content, dict):
                for sub_value in content.values():
                    if isinstance(sub_value, str):
                        collected_skills.add(sub_value)
            elif isinstance(content, str):
                # Split content into sentences to extract specific qualifications or skills
                lines = content.split('.')
                for line in lines:
                    if any(keyword in line.lower() for keyword in ["skill", "qualification", "certification"]):
                        collected_skills.add(line.strip())

    return ', '.join(collected_skills) if collected_skills else 'N/A'

# Step 2: Transform - Improved Experience Extraction
def extract_experience(job_data):
    experience = job_data.get('job_required_experience', {})
    
    if isinstance(experience, dict):
        years = experience.get('years', None)
        exp_description = experience.get('description', None)
        
        if years and exp_description:
            return f"{years} years - {exp_description}"
        elif years:
            return f"{years} years"
        elif exp_description:
            return exp_description
    elif isinstance(experience, str):
        return experience
    
    # Fallback: Check for experience-related information inside job descriptions
    job_description = job_data.get('job_description', '')
    if "experience" in job_description.lower():
        # Extract lines with 'experience' from the description
        exp_lines = [line.strip() for line in job_description.split('\n') if "experience" in line.lower()]
        if exp_lines:
            return ' '.join(exp_lines)

    return 'N/A'

# Step 2: Transform - Combine Responsibilities and Duties
def extract_responsibilities(job_data):
    responsibilities_text = job_data.get('job_description', 'N/A')
    if isinstance(responsibilities_text, dict):
        responsibilities_text = ' '.join([str(v) for v in responsibilities_text.values()])

    # Look for specific sections related to duties and responsibilities
    duty_lines = []
    lines = responsibilities_text.split('\n')
    capture = False

    for line in lines:
        lower_line = line.lower()
        if "duties" in lower_line or "responsibilities" in lower_line:
            capture = True  # Start capturing when we find relevant headers
        if capture:
            if "requirements" in lower_line:
                break  # Stop capturing when we hit unrelated sections
            duty_lines.append(line.strip())

    return ' '.join(duty_lines).strip() if duty_lines else 'N/A'

# Helper function to check job status
def check_status(job_data, key, keywords):
    if job_data.get(key, False):
        return 'Yes'
    else:
        description = job_data.get('job_description', '').lower()
        return 'Yes' if any(keyword in description for keyword in keywords) else 'No'

# Step 2: Transform - Extract On Site, Full Time, Remote, and Contractor Status
def extract_job_status(job_data):
    full_time = check_status(job_data, 'job_is_full_time', ['full-time'])
    remote = check_status(job_data, 'job_is_remote', ['remote'])
    contractor = check_status(job_data, 'job_is_contract', ['contractor'])
    on_site = check_status(job_data, 'job_is_on_site', ['on-site'])
    return full_time, remote, contractor, on_site

# Step 2: Transform - Processing the Extracted Data
def transform_job_data(jobs):
    job_list = []
    
    for job in jobs:
        title = clean_text(job.get('job_title', 'N/A'))
        company = clean_text(job.get('employer_name', 'N/A'))
        city = clean_text(job.get('job_city', 'N/A'))
        state = clean_text(job.get('job_state', 'N/A'))
        country = clean_text(job.get('job_country', 'N/A'))
        timestamp = job.get('job_posted_at_timestamp', None)
        date_posted = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d') if timestamp else 'N/A'

        # Extract Skills and Qualifications (enhanced details)
        skills_and_qualifications = clean_text(extract_skills_and_qualifications(job))

        # Responsibilities and Duties combined under Responsibilities
        responsibilities = clean_text(extract_responsibilities(job))

        # Experience (with improved handling of nested information)
        experience = clean_text(extract_experience(job))

        # Full Time / Remote / Contractor / On Site status
        full_time, remote, contractor, on_site = extract_job_status(job)

        # Extract the job application link
        job_url = job.get('job_apply_link', 'N/A')

        # Create a dictionary for each job
        job_list.append({
            "Job Title": title,
            "Company": company,
            "City": city,
            "State": state,
            "Country": country,
            "Date Posted": date_posted,
            "Skills and Qualifications": skills_and_qualifications,
            "Responsibilities": responsibilities,
            "Experience": experience,
            "Full Time": full_time,
            "Remote": remote,
            "Contractor": contractor,
            "On Site": on_site,
            "URL": job_url
        })

    return job_list

# Step 2 (New): Transform - Extract Years of Experience for ExperienceV2 Column
def extract_experience_years(experience_str):
    if pd.isnull(experience_str):
        return 'Unknown'
    
    match = re.search(r'(\d+)\s*years', experience_str)
    if match:
        return int(match.group(1))
    else:
        return 'Unknown'

# Step 3: Load - Writing Processed Data to CSV using Pandas and Renaming Columns
def load_data_to_csv(job_list, filepath):
    # Convert the list of job dictionaries into a DataFrame
    df = pd.DataFrame(job_list)
    
    # Rename columns by converting to lower case and replacing spaces with underscores
    new_columns = {col: col.lower().replace(' ', '_') for col in df.columns}
    df = df.rename(columns=new_columns)
    
    # Extract years of experience from the "experience" column
    df['ExperienceV2'] = df['experience'].apply(extract_experience_years)

    # Rename the "ExperienceV2" column to "years_experience"
    df = df.rename(columns={"ExperienceV2": "years_experience"})
    
    # Export the DataFrame to a CSV file
    df.to_csv(filepath, index=False)

# Main function to execute the ETL process
def main():
    # API request setup
    url = "https://jsearch.p.rapidapi.com/search"
    querystring = {
        "query": "cybersecurity jobs on glassdoor",
        "num_pages": "1",
        "date_posted": "all"
    }
    headers = {
        "x-rapidapi-key": "insert_key_here",
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }

    # Extract: Get job data from API for 5 pages
    raw_jobs = extract_data_from_api(url, headers, querystring, pages=5)

    if raw_jobs:
        # Transform: Process the extracted data
        transformed_jobs = transform_job_data(raw_jobs)

        # Load: Save the processed data to CSV with renamed columns and extracted experience data
        load_data_to_csv(transformed_jobs, "C:/Users/garne/Documents/cybersecurity_jobs_etl_pandas.csv")
    else:
        print("No job data found to process.")

# Run the main function
if __name__ == "__main__":
    main()

