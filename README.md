![AI_Image2](AI_Image2.jpeg)

# Cybersecurity Jobs Scraper Using RapidAPI and Python
This project is a Python-based web scraper that retrieves cybersecurity job listings using the RapidAPI Job Search API. It extracts key details about job listings such as the title, company, location, date posted, skills, qualifications, and other fields, then stores this information in a CSV file.

# Prerequisites
Ensure that the following are installed on your system before running the script:

# Python 3.x - Download Python
pip (Python package installer) - Comes with Python installation.
VS Code (Optional, but recommended) - Download VS Code
Installing Required Libraries
You need to install the required Python libraries before running the script.

Copy code
<!-- python code block -->
```python
pip install requests
```



API Setup
This script uses the RapidAPI Job Search API to get cybersecurity job listings. You'll need to sign up for a RapidAPI account and get an API key.

# Go to RapidAPI and sign up.
Subscribe to the Job Search API to get access to the API.
Find your API key under the Credentials section.
In the script, replace the x-rapidapi-key placeholder with your actual API key:

python
Copy code
headers = {
    "x-rapidapi-key": "your_rapidapi_key_here",
    "x-rapidapi-host": "jsearch.p.rapidapi.com"
}
# Running the Script in VS Code
Follow these steps to run the script:

# Open VS Code.
Open the folder where the Python script is located (File > Open Folder).
Install Python extension for VS Code if not already installed.
Open a terminal in VS Code (View > Terminal).
Run the script with:
bash
# Copy code
python your_script_name.py
Script Breakdown
Importing Required Libraries
The script starts by importing the necessary libraries for making the API request, parsing the JSON response, handling dates, and writing to a CSV file.

python
Copy code
import requests
import json
import csv
from datetime import datetime
Helper Function for Extracting Skills
The function extract_skills() is used to find and extract relevant qualifications and skills from the job data fields, such as job_description, job_required_skills, etc.

python
Copy code
def extract_skills(job_data):
    potential_keys = ['job_required_skills', 'skills', 'certifications', 'qualifications', 'job_description']
    collected_skills = []
    
    for key in potential_keys:
        content = job_data.get(key)
        if content:
            if isinstance(content, list):
                collected_skills.extend(content)
            elif 'qualification' in key.lower() or 'skill' in key.lower() or 'description' in key.lower():
                lines = content.split('.')
                for line in lines:
                    if "qualifications" in line.lower() or "skills" in line.lower():
                        collected_skills.append(line.strip())
    return ', '.join(collected_skills) if collected_skills else 'N/A'
# Setting Up the API Request
This section sets up the API endpoint, headers, and parameters. It makes a request to the RapidAPI Job Search API and checks whether the response was successful (status code 200).

python
Copy code
url = "https://jsearch.p.rapidapi.com/search"
querystring = {"query": "cybersecurity jobs on glassdoor", "page": "1", "num_pages": "1", "date_posted": "all"}

headers = {
    "x-rapidapi-key": "your_rapidapi_key_here",
    "x-rapidapi-host": "jsearch.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
Parsing and Saving the Job Data
If the request is successful, the response is parsed into JSON format, and relevant job data is extracted. A CSV file is created to store the information.

python
Copy code
if response.status_code == 200:
    data = response.json()

    with open("C:/Users/garne/Documents/cybersecurity_jobs_101_v3.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Write the headers
        writer.writerow(["Job Title", "Company", "City", "State", "Country", "Date Posted", "Skills", "Responsibilities", "Experience", "Qualifications", "On Site", "Remote", "Contractor", "URL"])

        # Assuming the jobs data is in the 'data' key
        if 'data' in data:
            jobs = data['data']

            # Iterate over the job results
            for job in jobs:
                title = job.get('job_title', 'N/A')
                company = job.get('employer_name', 'N/A')
                city = job.get('job_city', 'N/A')
                state = job.get('job_state', 'N/A')
                country = job.get('job_country', 'N/A')
                timestamp = job.get('job_posted_at_timestamp', None)
                date_posted = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d') if timestamp else 'N/A'

                # Extract Skills with enhanced details
                skills = extract_skills(job)

                responsibilities = job.get('job_description', 'N/A')
                experience = job.get('job_required_experience', {}).get('experience', 'N/A')
                qualifications = extract_skills(job)

                on_site = 'Yes' if job.get('job_is_on_site', False) else 'No'
                remote = 'Yes' if job.get('job_is_remote', False) else 'No'
                contractor = 'Yes' if job.get('job_is_contract', False) else 'No'

                job_url = job.get('job_apply_link', 'N/A')

                writer.writerow([title, company, city, state, country, date_posted, skills, responsibilities, experience, qualifications, on_site, remote, contractor, job_url])

        else:
            print("No job data found in the response.")
else:
    print(f"Failed to retrieve data: {response.status_code}")
Output
# The output CSV file will be saved in your local folder and contain the following columns:
Job Title
Company
City
State
Country
Date Posted
Skills
Responsibilities
Experience
Qualifications
On Site
Remote
Contractor
URL
# To access the CSV, navigate to the file path you specified in the code.

# Notes
API Rate Limits: Be aware of API rate limits imposed by RapidAPI to avoid exceeding your quota.
Error Handling: The script currently checks for successful responses but can be further enhanced with more detailed error handling.
Cybersecurity Jobs Scraper Using RapidAPI and Python
This project is a Python-based web scraper that retrieves cybersecurity job listings using the RapidAPI Job Search API. It extracts key details about job listings such as the title, company, location, date posted, skills, qualifications, and other fields, then stores this information in a CSV file.

Prerequisites
Ensure that the following are installed on your system before running the script:

Python 3.x - Download Python
pip (Python package installer) - Comes with Python installation.
VS Code (Optional, but recommended) - Download VS Code
Installing Required Libraries
You need to install the required Python libraries before running the script.

bash
Copy code
pip install requests
API Setup
This script uses the RapidAPI Job Search API to get cybersecurity job listings. You'll need to sign up for a RapidAPI account and get an API key.

Go to RapidAPI and sign up.
Subscribe to the Job Search API to get access to the API.
Find your API key under the Credentials section.
In the script, replace the x-rapidapi-key placeholder with your actual API key:

python
Copy code
headers = {
    "x-rapidapi-key": "your_rapidapi_key_here",
    "x-rapidapi-host": "jsearch.p.rapidapi.com"
}
Running the Script in VS Code
Follow these steps to run the script:

Open VS Code.
Open the folder where the Python script is located (File > Open Folder).
Install Python extension for VS Code if not already installed.
Open a terminal in VS Code (View > Terminal).
Run the script with:
bash
Copy code
python your_script_name.py
Script Breakdown
Importing Required Libraries
The script starts by importing the necessary libraries for making the API request, parsing the JSON response, handling dates, and writing to a CSV file.

python
Copy code
import requests
import json
import csv
from datetime import datetime
Helper Function for Extracting Skills
The function extract_skills() is used to find and extract relevant qualifications and skills from the job data fields, such as job_description, job_required_skills, etc.

python
Copy code
def extract_skills(job_data):
    potential_keys = ['job_required_skills', 'skills', 'certifications', 'qualifications', 'job_description']
    collected_skills = []
    
    for key in potential_keys:
        content = job_data.get(key)
        if content:
            if isinstance(content, list):
                collected_skills.extend(content)
            elif 'qualification' in key.lower() or 'skill' in key.lower() or 'description' in key.lower():
                lines = content.split('.')
                for line in lines:
                    if "qualifications" in line.lower() or "skills" in line.lower():
                        collected_skills.append(line.strip())
    return ', '.join(collected_skills) if collected_skills else 'N/A'
Setting Up the API Request
This section sets up the API endpoint, headers, and parameters. It makes a request to the RapidAPI Job Search API and checks whether the response was successful (status code 200).

python
Copy code
url = "https://jsearch.p.rapidapi.com/search"
querystring = {"query": "cybersecurity jobs on glassdoor", "page": "1", "num_pages": "1", "date_posted": "all"}

headers = {
    "x-rapidapi-key": "your_rapidapi_key_here",
    "x-rapidapi-host": "jsearch.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
Parsing and Saving the Job Data
If the request is successful, the response is parsed into JSON format, and relevant job data is extracted. A CSV file is created to store the information.

python
Copy code
if response.status_code == 200:
    data = response.json()

    with open("C:/Users/garne/Documents/cybersecurity_jobs_101_v3.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Write the headers
        writer.writerow(["Job Title", "Company", "City", "State", "Country", "Date Posted", "Skills", "Responsibilities", "Experience", "Qualifications", "On Site", "Remote", "Contractor", "URL"])

        # Assuming the jobs data is in the 'data' key
        if 'data' in data:
            jobs = data['data']

            # Iterate over the job results
            for job in jobs:
                title = job.get('job_title', 'N/A')
                company = job.get('employer_name', 'N/A')
                city = job.get('job_city', 'N/A')
                state = job.get('job_state', 'N/A')
                country = job.get('job_country', 'N/A')
                timestamp = job.get('job_posted_at_timestamp', None)
                date_posted = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d') if timestamp else 'N/A'

                # Extract Skills with enhanced details
                skills = extract_skills(job)

                responsibilities = job.get('job_description', 'N/A')
                experience = job.get('job_required_experience', {}).get('experience', 'N/A')
                qualifications = extract_skills(job)

                on_site = 'Yes' if job.get('job_is_on_site', False) else 'No'
                remote = 'Yes' if job.get('job_is_remote', False) else 'No'
                contractor = 'Yes' if job.get('job_is_contract', False) else 'No'

                job_url = job.get('job_apply_link', 'N/A')

                writer.writerow([title, company, city, state, country, date_posted, skills, responsibilities, experience, qualifications, on_site, remote, contractor, job_url])

        else:
            print("No job data found in the response.")
else:
    print(f"Failed to retrieve data: {response.status_code}")
Output
The output CSV file will be saved in your local folder and contain the following columns:
Job Title
Company
City
State
Country
Date Posted
Skills
Responsibilities
Experience
Qualifications
On Site
Remote
Contractor
URL
To access the CSV, navigate to the file path you specified in the code.

Notes
API Rate Limits: Be aware of API rate limits imposed by RapidAPI to avoid exceeding your quota.
Error Handling: The script currently checks for successful responses but can be further enhanced with more detailed error handling.
- 👋 Hi, I’m @ravenmyst110011

<!---
ravenmyst110011/ravenmyst110011 is a ✨ special ✨ repository because its `README.md` (this file) appears on your GitHub profile.
You can click the Preview link to take a look at your changes.
--->
