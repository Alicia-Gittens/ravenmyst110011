import requests
import json
import csv
from datetime import datetime

# Helper function to extract skills and qualifications
def extract_skills(job_data):
    potential_keys = ['job_required_skills', 'skills', 'certifications', 'qualifications', 'job_description']
    collected_skills = []
    
    for key in potential_keys:
        content = job_data.get(key)
        if content:
            if isinstance(content, list):
                collected_skills.extend(content)  # Extend list if content is a list
            elif 'qualification' in key.lower() or 'skill' in key.lower() or 'description' in key.lower():
                lines = content.split('.')
                for line in lines:
                    if "qualifications" in line.lower() or "skills" in line.lower():
                        collected_skills.append(line.strip())
    return ', '.join(collected_skills) if collected_skills else 'N/A'

# API request setup
url = "https://jsearch.p.rapidapi.com/search"
querystring = {"query": "cybersecurity jobs on glassdoor", "page": "1", "num_pages": "1", "date_posted": "all"}

headers = {
    "x-rapidapi-key": "insert your api key here",
    "x-rapidapi-host": "jsearch.p.rapidapi.com"
}

# Make the request to the API
response = requests.get(url, headers=headers, params=querystring)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Open or create a CSV file to write job data
    with open("C:/Users/garne/Documents/cybersecurity_jobs_101_v3.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Write the headers
        writer.writerow(["Job Title", "Company", "City", "State", "Country", "Date Posted", "Skills", "Responsibilities", "Experience", "Qualifications", "On Site", "Remote", "Contractor", "URL"])

        # Assuming the jobs data is in the 'data' key
        if 'data' in data:
            jobs = data['data']

            # Iterate over the job results
            for job in jobs:
                # Extract the relevant information from each job
                title = job.get('job_title', 'N/A')
                company = job.get('employer_name', 'N/A')
                city = job.get('job_city', 'N/A')
                state = job.get('job_state', 'N/A')
                country = job.get('job_country', 'N/A')
                timestamp = job.get('job_posted_at_timestamp', None)
                date_posted = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d') if timestamp else 'N/A'

                # Extract Skills with enhanced details
                skills = extract_skills(job)

                # Responsibilities (inferred from job description)
                responsibilities = job.get('job_description', 'N/A')

                # Experience (nested inside 'job_required_experience')
                experience = job.get('job_required_experience', {}).get('experience', 'N/A')

                # Qualifications (extracted and processed separately)
                qualifications = extract_skills(job)  # Reusing the function to get detailed qualifications too

                # On Site / Remote / Contractor status
                on_site = 'Yes' if job.get('job_is_on_site', False) else 'No'
                remote = 'Yes' if job.get('job_is_remote', False) else 'No'
                contractor = 'Yes' if job.get('job_is_contract', False) else 'No'

                # Extract the job application link
                job_url = job.get('job_apply_link', 'N/A')

                # Write the row to the CSV
                writer.writerow([title, company, city, state, country, date_posted, skills, responsibilities, experience, qualifications, on_site, remote, contractor, job_url])

        else:
            print("No job data found in the response.")

else:
    print(f"Failed to retrieve data: {response.status_code}")

