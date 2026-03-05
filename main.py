import time
import random
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
from selenium import webdriver
from time import sleep
import random
import os
import unicodedata
import re
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import json
from selenium.webdriver.chrome.options import Options
import os
import requests
from enum import Enum
from typing import List, Optional, Any
from google.cloud import bigquery
from google.oauth2 import service_account



# Set up Selenium options (headless mode for efficiency)
options = Options()
options.add_argument("--headless=new")  # modern headless mode
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--remote-debugging-port=9222")  # helps Chrome bind properly in CI
options.add_argument("--window-size=1920,1080")

# Initialize WebDriver
driver = webdriver.Chrome(options=options)

base_url = "https://careers.societegenerale.com/en/search?refinementList[jobLocation][0]=AUS_A01&refinementList[jobLocation][1]=BEL_A01&refinementList[jobLocation][2]=CHN_A07&refinementList[jobLocation][3]=CHN_A11&refinementList[jobLocation][4]=ESP_A02&refinementList[jobLocation][5]=FRA_A20_D001_L001&refinementList[jobLocation][6]=FRA_A20_D005_L025&refinementList[jobLocation][7]=FRA_A23_D004&refinementList[jobLocation][8]=GBR_A01&refinementList[jobLocation][9]=GER_A08_D003&refinementList[jobLocation][10]=HKG_A01&refinementList[jobLocation][11]=IND_A37&refinementList[jobLocation][12]=KOR_A01&refinementList[jobLocation][13]=LUX_A01&refinementList[jobLocation][14]=NLD_A01&refinementList[jobLocation][15]=POL_A01&refinementList[jobLocation][16]=USA_A07_D001&refinementList[jobFunction][0]=PF251&refinementList[jobFunction][1]=UW387&refinementList[jobFunction][2]=ZX468&refinementList[jobFunction][3]=KJ697&refinementList[jobFunction][4]=NP922&refinementList[jobFunction][5]=DF734&refinementList[jobFunction][6]=IM205&refinementList[jobFunction][7]=SU264&refinementList[jobFunction][8]=ET461"
max_pages = 1
job_urls = []

wait_time = random.uniform(5, 10)

for page in range(1, max_pages + 1):
    url = f"{base_url}&page={page}"
    driver.get(url)  # Open the page
    time.sleep(wait_time)
    print(url)
        
    # Wait for job items to be present (Optional: You can add WebDriverWait if needed)
    job_links = driver.find_elements(By.CSS_SELECTOR, "div.search-job-list a.js-link-job")
        
    for link in job_links:
        href = link.get_attribute("href")
        job_urls.append(href)

driver.quit()  # Close the browser when done

print(f"Collected {len(job_urls)} job URLs")

#------------------------CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

# Query existing URLs from your BigQuery table
query = """
    SELECT url
    FROM `databasealfred.alfredFinance.societeGenerale`
    WHERE url IS NOT NULL
"""
query_job = client.query(query)

# Convert results to a set for fast lookup
existing_urls = {row.url for row in query_job}

print(f"Loaded {len(existing_urls)} URLs from BigQuery")

# Filter job_urls
job_urls = [url for url in job_urls if url not in existing_urls]

print(f"✅ Remaining job URLs to scrape: {len(job_urls)}")


#------------------------ FIN CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------


# Set up Selenium options (headless mode for efficiency)
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# Initialize WebDriver
driver = webdriver.Chrome(options=options)

# Initialize an empty list to store job data
job_data = []


for job_url in job_urls:
    driver.get(job_url)

    def get_text(selector, multiple=False):
        """Helper function to extract text from an element."""
        try:
            if multiple:
                return [elem.text.strip() for elem in driver.find_elements(By.CSS_SELECTOR, selector)]
            return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
        except NoSuchElementException:
            return "" if not multiple else []


    description = get_text('div.lg\\:col-span-2')
    division = get_text('div.inline-flex.border')
    
    location = get_text('div.mask-location-check')
    location = location.split(",")[0].strip()
    
    #experienceLevel = get_text('div[data-testid="opportunity-field-corporateTitle"] span.gs-text:last-child')
    scrappedDateTime = datetime.datetime.now().isoformat()
    scrappedDate = datetime.datetime.now().strftime("%Y-%m-%d")
    scrappedHour = datetime.datetime.now().strftime("%H")
    scrappedMinutes = datetime.datetime.now().strftime("%M")
    title = driver.find_element(By.CSS_SELECTOR, 'h1').text.strip()

    contract = get_text("div.bg-pink-800 span")
    
    print(title)


    # Append extracted data to list
    job_data.append({
        "title": title,
        "location": location,
        "scrappedDateTime": scrappedDateTime,
        "description": description,
        "division": division,
        "experienceLevel": "",
        "url": job_url,
        "source":"Societe Generale",
        "scrappedDate": scrappedDate,
        "scrappedHour": scrappedHour,
        "scrappedMinutes": scrappedMinutes,
        "scrappedDateTimeText": scrappedDateTime,
        "contract": contract
    })

# Convert list to Pandas DataFrame
df_jobs = pd.DataFrame(job_data)

# Convert scraped results into a DataFrame
new_data = df_jobs

import re
import numpy as np

def extract_experience_level(title):
    if pd.isna(title):
        return ""
    
    title = title.lower()

    patterns = [
        (r'\bsummer\s+analyst\b|\bsummer\s+analyste\b', "Summer Analyst"),
        (r'\bsummer\s+associate\b|\bsummer\s+associé\b', "Summer Associate"),
        (r'\bvice\s+president\b|\bsvp\b|\bvp\b|\bprincipal\b', "Vice President"),
        (r'\bassistant\s+vice\s+president\b|\bsavp\b|\bavp\b', "Assistant Vice President"),
        (r'\bsenior\s+manager\b', "Senior Manager"),
        (r'\bproduct\s+manager\b|\bpm\b|\bmanager\b', "Manager"),
        (r'\bmanager\b', "Manager"),
        (r'\bengineer\b|\bengineering\b', "Engineer"),
        (r'\badministrative\s+assistant\b|\bexecutive\s+assistant\b|\badmin\b', "Assistant"),
        (r'\bassociate\b|\bassocié\b', "Associate"),
        (r'\banalyst\b|\banalyste\b|\banalist\b', "Analyst"),
        (r'\bchief\b|\bhead\b', "C-Level"),
        (r'\bV.I.E\b|\bVIE\b|\bvolontariat international\b|\bV I E\b|', "VIE"),
    ]

    for pattern, label in patterns:
        if re.search(pattern, title):
            return label

    return "" 

# Apply to dataframe
new_data["experienceLevel"] = new_data["title"].apply(extract_experience_level)
new_data.loc[new_data['contract'].isin(['Internship', 'Stage']), 'experienceLevel'] = 'Intern'
new_data = new_data.drop(columns=['contract'])

#---------UPLOAD TO BIGQUERY-------------------------------------------------------------------------------------------------------------

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

table_id = "databasealfred.alfredFinance.societeGenerale"

# CONFIG WITHOUT PYARROW
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

# Convert DataFrame → list of dict rows (JSON compatible)
rows = new_data.to_dict(orient="records")

# Upload
job = client.load_table_from_json(
    rows,
    table_id,
    job_config=job_config
)

job.result()
