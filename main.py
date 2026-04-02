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
from zoneinfo import ZoneInfo


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
max_pages = 5
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


    sections_text = get_text('[id^="job-detail"]', multiple=True)
    description = "\n".join(sections_text)
    
    division = get_text('div.inline-flex.border')
    
    location = get_text('div.mask-location-check')
    location = location.split(",")[0].strip()
    
    #experienceLevel = get_text('div[data-testid="opportunity-field-corporateTitle"] span.gs-text:last-child')

    paris_now = datetime.datetime.now(ZoneInfo("Europe/Paris"))
    scrappedDateTime    = paris_now.isoformat()
    scrappedDate        = paris_now.strftime("%Y-%m-%d")
    scrappedHour        = paris_now.strftime("%H")
    scrappedMinutes     = paris_now.strftime("%M")
    
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

from rapidfuzz import process, fuzz
from difflib import get_close_matches

# -------------------------------
# 1. Base mapping (same as before)
# -------------------------------

BASE_MAPPING = {

    # ================= INVESTMENT BANKING =================
    "investment banking": "Investment Banking (M&A / Advisory)",
    "m&a": "Investment Banking (M&A / Advisory)",
    "mergers and acquisitions": "Investment Banking (M&A / Advisory)",
    "corporate finance": "Investment Banking (M&A / Advisory)",
    "ecm": "Investment Banking (M&A / Advisory)",
    "dcm": "Investment Banking (M&A / Advisory)",
    "capital markets origination": "Investment Banking (M&A / Advisory)",
    "corporate & investment banking": "Investment Banking (M&A / Advisory)",
    "banque de financement et d'investissement": "Investment Banking (M&A / Advisory)",
    "investment banking (m&a / advisory)": "Investment Banking (M&A / Advisory)",

    # ================= MARKETS =================
    "markets": "Markets (Sales & Trading)",
    "sales and trading": "Markets (Sales & Trading)",
    "trading": "Markets (Sales & Trading)",
    "sales": "Markets (Sales & Trading)",
    "structuring": "Markets (Sales & Trading)",
    "derivatives": "Markets (Sales & Trading)",
    "fixed income": "Markets (Sales & Trading)",
    "equities": "Markets (Sales & Trading)",
    "fx": "Markets (Sales & Trading)",
    "global markets": "Markets (Sales & Trading)",
    "global banking & markets": "Markets (Sales & Trading)",
    "sales development": "Markets (Sales & Trading)",
    "developpement commercial": "Markets (Sales & Trading)",
    "desarrollo comercial": "Markets (Sales & Trading)",
    "customer relationship management": "Markets (Sales & Trading)",
    "markets (sales & trading)": "Markets (Sales & Trading)",

    # ================= ASSET & WEALTH =================
    "asset & wealth management": "Asset & Wealth Management",
    "asset management": "Asset & Wealth Management",
    "wealth management": "Asset & Wealth Management",
    "gestion d'actifs": "Asset & Wealth Management",
    "gestion de patrimoine": "Asset & Wealth Management",
    "portfolio management": "Asset & Wealth Management",
    "investment management": "Asset & Wealth Management",
    "private wealth": "Asset & Wealth Management",
    "private banking": "Asset & Wealth Management",
    "banque privée": "Asset & Wealth Management",
    "retail banking": "Asset & Wealth Management",
    "retail & online banking": "Asset & Wealth Management",
    "réseau d'agences et banque en ligne": "Asset & Wealth Management",
    "asset & wealth management": "Asset & Wealth Management",

    # ================= PRIVATE EQUITY =================
    "private equity": "Private Equity & Alternatives",
    "alternatives": "Private Equity & Alternatives",
    "growth equity": "Private Equity & Alternatives",
    "venture capital": "Private Equity & Alternatives",
    "buyout": "Private Equity & Alternatives",
    "lbo": "Private Equity & Alternatives",
    "multi-asset investing (bxma)": "Private Equity & Alternatives",
    "private equity & alternatives": "Private Equity & Alternatives",

    # ================= CREDIT =================
    "credit": "Credit & Lending",
    "lending": "Credit & Lending",
    "leveraged finance": "Credit & Lending",
    "structured finance": "Credit & Lending",
    "corporate treasury": "Credit & Lending",
    "blackstone credit & insurance": "Credit & Lending",
    "credit & lending": "Credit & Lending",

    # ================= RESEARCH =================
    "research": "Research & Strategy",
    "equity research": "Research & Strategy",
    "credit research": "Research & Strategy",
    "macro research": "Research & Strategy",
    "global investment research division": "Research & Strategy",
    "research & strategy": "Research & Strategy",

    # ================= RISK =================
    "risk": "Risk Management",
    "risk management": "Risk Management",
    "market risk": "Risk Management",
    "credit risk": "Risk Management",
    "operational risk": "Risk Management",
    "enterprise risk": "Risk Management",
    "risque": "Risk Management",
    "riesgos": "Risk Management",
    "risks": "Risk Management",
    "risques": "Risk Management",
    "risk division": "Risk Management",
    "risk management": "Risk Management",

    # ================= COMPLIANCE =================
    "compliance": "Compliance & Financial Crime",
    "financial crime": "Compliance & Financial Crime",
    "aml": "Compliance & Financial Crime",
    "kyc": "Compliance & Financial Crime",
    "regulatory compliance": "Compliance & Financial Crime",
    "compliance division": "Compliance & Financial Crime",
    "conformite": "Compliance & Financial Crime",
    "legal and compliance": "Compliance & Financial Crime",
    "conflicts resolution group": "Compliance & Financial Crime",
    "compliance & financial crime": "Compliance & Financial Crime",

    # ================= FINANCE =================
    "finance": "Finance (Accounting / Controlling / Tax)",
    "accounting": "Finance (Accounting / Controlling / Tax)",
    "controlling": "Finance (Accounting / Controlling / Tax)",
    "fp&a": "Finance (Accounting / Controlling / Tax)",
    "financial planning": "Finance (Accounting / Controlling / Tax)",
    "management control": "Finance (Accounting / Controlling / Tax)",
    "finance accounts and management control": "Finance (Accounting / Controlling / Tax)",
    "finance comptabilite et controle de gestion": "Finance (Accounting / Controlling / Tax)",
    "contabilidad financiera y control de gestión": "Finance (Accounting / Controlling / Tax)",
    "controllers": "Finance (Accounting / Controlling / Tax)",
    "tax": "Finance (Accounting / Controlling / Tax)",
    "financial and technical expertise": "Finance (Accounting / Controlling / Tax)",
    "expertise financiere et technique": "Finance (Accounting / Controlling / Tax)",
    "expertise financiero y técnico": "Finance (Accounting / Controlling / Tax)",
    "financieel en technisch experts": "Finance (Accounting / Controlling / Tax)",
    "finance (accounting / controlling / tax)": "Finance (Accounting / Controlling / Tax)",

    # ================= OPERATIONS =================
    "operations": "Operations (Back/Middle Office)",
    "middle office": "Operations (Back/Middle Office)",
    "back office": "Operations (Back/Middle Office)",
    "operation processing": "Operations (Back/Middle Office)",
    "trade support": "Operations (Back/Middle Office)",
    "settlement": "Operations (Back/Middle Office)",
    "clearing": "Operations (Back/Middle Office)",
    "reconciliation": "Operations (Back/Middle Office)",
    "traitement des operations": "Operations (Back/Middle Office)",
    "gestión de operaciones": "Operations (Back/Middle Office)",
    "gestion des opérations bancaires": "Operations (Back/Middle Office)",
    "operations division": "Operations (Back/Middle Office)",
    "banking operations processing": "Operations (Back/Middle Office)",
    "portfolio operations": "Operations (Back/Middle Office)",
    "platform solutions": "Operations (Back/Middle Office)",
    "operations (back/middle office)": "Operations (Back/Middle Office)",

    # ================= AUDIT =================
    "audit": "Audit & Internal Control",
    "internal control": "Audit & Internal Control",
    "audit / control / quality": "Audit & Internal Control",
    "audit / contrôle / qualité": "Audit & Internal Control",
    "internal audit": "Audit & Internal Control",
    "permanent control": "Audit & Internal Control",
    "controle permanent": "Audit & Internal Control",
    "ics": "Audit & Internal Control",
    "audit & internal control": "Audit & Internal Control",

    # ================= TECHNOLOGY =================
    "technology": "Technology (IT / Engineering)",
    "it": "Technology (IT / Engineering)",
    "data": "Technology (IT / Engineering)",
    "data science": "Technology (IT / Engineering)",
    "engineering": "Technology (IT / Engineering)",
    "software": "Technology (IT / Engineering)",
    "information technology": "Technology (IT / Engineering)",
    "engineering division": "Technology (IT / Engineering)",
    "infrastructure": "Technology (IT / Engineering)",
    "technology (it / engineering)": "Technology (IT / Engineering)",

    # ================= CORPORATE =================
    "corporate functions": "Corporate Functions",
    "corporate affairs": "Corporate Functions",
    "communications": "Corporate Functions",
    "marketing": "Corporate Functions",
    "global corporate services": "Corporate Functions",
    "security or facilities management": "Corporate Functions",
    "procurement": "Corporate Functions",
    "purchasing / procurement": "Corporate Functions",
    "human resources": "Corporate Functions",
    "human capital management": "Corporate Functions",
    "human capital management division": "Corporate Functions",
    "strategic partners": "Corporate Functions",
    "corporate functions": "Corporate Functions",

    # ================= STRATEGY =================
    "executive": "Executive / Strategy / Management",
    "management": "Executive / Strategy / Management",
    "strategy": "Executive / Strategy / Management",
    "consulting": "Executive / Strategy / Management",
    "executive office division": "Executive / Strategy / Management",
    "pilotage": "Executive / Strategy / Management",
    "steering": "Executive / Strategy / Management",
    "executive / strategy / management": "Executive / Strategy / Management",

    # ================= REAL ESTATE =================
    "real estate": "Real Estate",
    "real assets": "Real Estate",

    # ================= OTHER =================
    "temporary": "Other / Temporary",
    "temporary status": "Other / Temporary",
    "statuts temporaires": "Other / Temporary",
    "miscellaneous": "Other / Temporary",
    "other": "Other / Temporary",
    "division": "Other / Temporary",
    "other / temporary": "Other / Temporary",
}

# -------------------------------
# 2. Precompute keys
# -------------------------------
KNOWN_DIVISIONS = list(BASE_MAPPING.keys())

# -------------------------------
# 3. Fuzzy-enhanced mapper
# -------------------------------
def map_division_fuzzy(value: str, threshold: int = 85) -> str:
    if not value:
        return "Other / Temporary"

    v = str(value).strip().lower()

    # 1. Exact match
    if v in BASE_MAPPING:
        return BASE_MAPPING[v]

    # 2. Fuzzy match
    match, score, _ = process.extractOne(
        v,
        KNOWN_DIVISIONS,
        scorer=fuzz.token_sort_ratio
    )

    if score >= threshold:
        return BASE_MAPPING[match]

    # 3. Fallback
    return "Other / Temporary"


# -------------------------------
# 4. Apply to DataFrame
# -------------------------------
new_data["division"] = new_data["division"].apply(map_division_fuzzy)

# -------------------------------
# 1. BASE CITY MAPPING
# -------------------------------
BASE_CITY_MAPPING = {
    # USA
    "albany": "Albany",
    "atlanta": "Atlanta",
    "boston": "Boston",
    "chicago": "Chicago",
    "dallas": "Dallas",
    "detroit": "Detroit",
    "houston": "Houston",
    "los angeles": "Los Angeles",
    "minneapolis": "Minneapolis",
    "new york": "New York",
    "new york city": "New York",
    "new york & jersey city": "New York",
    "new york, new jersey": "New York",
    "new york, chicago": "New York",
    "new york 601 lex": "New York",
    "morristown": "Morristown",
    "philadelphia": "Philadelphia",
    "pittsburgh": "Pittsburgh",
    "norwalk": "Norwalk",
    "west palm beach": "West Palm Beach",
    "richardson": "Richardson",
    "richardson, dallas": "Richardson",

    # Canada
    "calgary": "Calgary",
    "montreal": "Montreal",
    "montéal": "Montreal",
    "monteal": "Montreal",
    "chesterbrook": "Chesterbrook",
    "toronto": "Toronto",

    # Europe
    "amsterdam": "Amsterdam",
    "berlin": "Berlin",
    "frankfurt": "Frankfurt",
    "frankfurt am main": "Frankfurt",
    "frankfurt omniturm": "Frankfurt",
    "birmingham": "Birmingham",
    "brussels": "Brussels",
    "bruxelles": "Brussels",
    "budapest": "Budapest",
    "geneva": "Geneva",
    "genève": "Geneva",
    "genève (lancy)": "Geneva",
    "glasgow": "Glasgow",
    "hannover": "Hanover",
    "lisbon": "Lisbon",
    "lisboa": "Lisbon",
    "lisboa or porto": "Lisbon",
    "lisboa/porto": "Lisbon",
    "porto": "Porto",
    "porto / lisbon": "Porto",
    "porto or lisbon": "Porto",
    "madrid": "Madrid",
    "milano": "Milan",
    "milan": "Milan",
    "munich": "Munich",
    "krakow": "Krakow",
    "kraków": "Krakow",
    "warsaw": "Warsaw",
    "warszawa": "Warsaw",
    "rome": "Rome",
    "roma": "Rome",
    "stockholm": "Stockholm",
    "zurich": "Zurich",
    "zürich": "Zurich",
    "pantin": "Pantin",
    "nanterre": "Nanterre",
    "la defense": "Paris",
    "montreuil (idf)": "Paris",
    "ile de france": "Paris",
    "paris": "Paris",

    # Middle East
    "doha": "Doha",
    "dubai": "Dubai",
    "riyadh": "Riyadh",
    "tel aviv": "Tel Aviv",

    # Asia
    "auckland": "Auckland",
    "bangalore": "Bangalore",
    "bengaluru": "Bangalore",
    "bengalutru": "Bangalore",
    "beijing": "Beijing",
    "chennai": "Chennai",
    "ho chi minh": "Ho Chi Minh City",
    "hong kong": "Hong Kong",
    "hyderabad": "Hyderabad",
    "mumbai": "Mumbai",
    "mumbai/ bangalore": "Mumbai",
    "minato-ku": "Tokyo",
    "seoul": "Seoul",
    "shanghai": "Shanghai",
    "singapore": "Singapore",
    "singapour": "Singapore",
    "taipei": "Taipei",
    "tokyo": "Tokyo",
    "xinyi district": "Taipei",

    # Latin America
    "bogota": "Bogota",
    "buenos aires": "Buenos Aires",
    "ciudad de mexico": "Mexico City",
    "metro manila": "Metro Manila",
    "sao paulo": "Sao Paulo",
    "são paulo": "Sao Paulo",
    "region metropolitana de santiago": "Santiago",

    # Oceania
    "sydney": "Sydney",
    "auckland": "Auckland",

    # Other / Remote
    "remote - deutschlandweit": "Remote",
    "ireland": "Dublin",
    "dublin": "Dublin",
    "galway": "Dublin",
    "dublin or galway": "Dublin",
    "galway/dublin": "Dublin",
    "jersey city": "Jersey City",
    "berkeley square house london": "London",
}

# -------------------------------
# 2. SELF-MAPPING
# -------------------------------
CITY_CATEGORIES = set(BASE_CITY_MAPPING.values())
BASE_CITY_MAPPING.update({city.lower(): city for city in CITY_CATEGORIES})

# -------------------------------
# 3. FUZZY MATCHING
# -------------------------------
KNOWN_LOCATIONS = list(BASE_CITY_MAPPING.keys())

def map_location(value: str, cutoff: float = 0.8) -> str:
    if not value:
        return "Other / Unknown"

    v = str(value).strip().lower()

    # Exact match
    if v in BASE_CITY_MAPPING:
        return BASE_CITY_MAPPING[v]

    # Fuzzy match
    matches = get_close_matches(v, KNOWN_LOCATIONS, n=1, cutoff=cutoff)
    if matches:
        return BASE_CITY_MAPPING[matches[0]]

    return value

# -------------------------------
# 4. APPLY TO DATAFRAME
# -------------------------------
new_data["location"] = new_data["location"].apply(map_location)

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
