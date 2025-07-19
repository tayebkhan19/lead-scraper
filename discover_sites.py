"""
A scalable script that discovers Indian e-commerce websites using a
manual phrase system, analyzes them in parallel, and saves the results.
"""
# --- IMPORTS ---
import json
import os
import random
import re
import time
from urllib.parse import urljoin, urlparse, urlunparse
import gspread
import requests
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import phonenumbers

# --- LOGGING SETUP ---
# (This section is unchanged)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='lead_discovery.log',
    filemode='w'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


# --- CONFIGURATION ---
# (This section is unchanged)
SEARCH_CONFIG_FILE = "search_phrases.json"
GOOGLE_SHEET_NAME = os.getenv("GSHEET_NAME", "Scraped Leads")
GOOGLE_CREDS_FILE = "credentials.json"
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36',
]
BLACKLISTED_DOMAINS = [
    'amazon.com', 'flipkart.com', 'myntra.com', 'ajio.com', 'meesho.com', 'nykaa.com',
]
NEGATIVE_PATH_KEYWORDS = ['blog', 'news', 'docs', 'forum', 'support', 'publication', 'careers']
NEGATIVE_CONTENT_KEYWORDS = ['market research', 'consulting firm']
INDIAN_TECH_KEYWORDS = ['razorpay', 'payu', 'instamojo', 'shiprocket', 'delhivery']
POLICY_PAGE_HINTS = ['contact', 'about', 'legal', 'policy', 'shipping', 'terms']
SOCIAL_MEDIA_DOMAINS = ['facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com']


# --- SETUP, UTILITY, & SAVING FUNCTIONS (MODIFIED) ---
def setup_google_sheet():
    """Connects to Google Sheets and prepares both the main and log worksheets."""
    try:
        logging.info("Connecting to Google Sheets...")
        gc = gspread.service_account(filename=GOOGLE_CREDS_FILE)
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        
        # Setup main leads sheet
        leads_worksheet = spreadsheet.sheet1
        if not leads_worksheet.get_all_values():
            leads_worksheet.append_row([
                "URL", "Email", "Phone Number", "Facebook", "Instagram",
                "Twitter", "LinkedIn", "Scraped Timestamp"
            ])
            
        # Setup the "Run Logs" sheet
        try:
            logs_worksheet = spreadsheet.worksheet("Run Logs")
        except gspread.WorksheetNotFound:
            logs_worksheet = spreadsheet.add_worksheet(title="Run Logs", rows="100", cols="20")
            logs_worksheet.append_row(["Timestamp", "Total Leads Found", "Summary"])

        logging.info("Google Sheets connection successful.")
        return leads_worksheet, logs_worksheet

    except Exception as e:
        logging.error(f"❌ Google Sheets Error: {e}")
        return None, None

# --- NEW: FUNCTION TO SAVE LOGS TO GOOGLE SHEET ---
def save_log_to_gsheet(worksheet, total_leads, summary_text):
    """Saves a summary of the run to the 'Run Logs' worksheet."""
    try:
        logging.info("Saving run summary to the log sheet...")
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        worksheet.append_row([timestamp, total_leads, summary_text])
        logging.info("✅  Run summary saved successfully.")
    except Exception as e:
        logging.error(f"❌ Could not save run summary to Google Sheet: {e}")

# ... (The rest of your utility, data extraction, and analysis functions are unchanged) ...
def get_existing_urls_from_sheet(worksheet):
    try:
        logging.info("Fetching existing URLs from Google Sheet...")
        urls = worksheet.col_values(1)
        logging.info(f"Found {len(urls) -1} existing URLs in the sheet.")
        return set(urls[1:])
    except Exception as e:
        logging.error(f"❌ Could not fetch existing URLs from sheet: {e}"); return set()

def clean_and_validate_url(url):
    try:
        match = re.search(r'https?://[^\s?#]+', url)
        if not match: return None
        url_to_parse = match.group(0)
        if any(f"/{keyword}" in url_to_parse for keyword in NEGATIVE_PATH_KEYWORDS): return None
        parsed = urlparse(url_to_parse)
        cleaned_url = urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))
        domain = parsed.netloc.replace('www.', '')
        if any(blacklisted in domain for blacklisted in BLACKLISTED_DOMAINS): return None
        return cleaned_url
    except Exception: return None

def save_to_gsheet(worksheet, lead_data):
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        socials = lead_data.get("social_links", {})
        worksheet.append_row([
            lead_data.get("URL", "Not Found"), lead_data.get("Email", "Not Found"),
            lead_data.get("Phone Number", "Not Found"), socials.get("facebook", "Not Found"),
            socials.get("instagram", "Not Found"), socials.get("twitter", "Not Found"),
            socials.get("linkedin", "Not Found"), timestamp
        ])
        logging.info(f"✅  Saved to Google Sheet: {lead_data.get('URL')}")
    except gspread.exceptions.APIError as e:
        logging.error(f"❌ Could not write to Google Sheet due to API error: {e}"); time.sleep(60)

def _extract_email(text):
    match = re.search(r'[\w\.\-]+@[\w\.\-]+\.\w+', text)
    return match.group(0) if match else "Not Found"

def _extract_phone_number(text):
    for match in phonenumbers.PhoneNumberMatcher(text, "IN"):
        return phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
    return "Not Found"

def _extract_social_links(soup):
    social_links = {}
    for a in soup.find_all('a', href=True):
        href = a['href']
        for domain in SOCIAL_MEDIA_DOMAINS:
            platform = domain.split('.')[0]
            if domain in href and platform not in social_links:
                social_links[platform] = href
    return social_links

def analyze_site(url):
    logging.info(f"   Analyzing {url}...")
    try:
        time.sleep(random.uniform(1.0, 3.0))
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        response = requests.get(url, headers=headers, timeout=15, verify=True)
        response.raise_for_status()
        html_text, soup, score = response.text.lower(), BeautifulSoup(response.text, 'html.parser'), 0
        if any(soup.find(['button', 'a', 'input'], text=re.compile(r'add to cart|buy now|shop now', re.I))) or \
           len(soup.find_all(attrs={'class': re.compile(r'product|item|grid|listing', re.I)})) >= 3 or \
           '"@type":"product"' in html_text or any(tag in html_text for tag in ['shopify', 'woocommerce']):
            score += 1
        else:
            logging.info("   [FAIL] No strong e-commerce signals found."); return None
        if any(keyword in html_text for keyword in NEGATIVE_CONTENT_KEYWORDS):
            logging.warning("   [FAIL] Contains blacklisted content keyword."); return None
        page_html_to_check = html_text
        for hint in POLICY_PAGE_HINTS:
            page_link = soup.find('a', href=re.compile(hint), text=re.compile(hint, re.I))
            if page_link and page_link.get('href'):
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    page_html_to_check += requests.get(urljoin(url, page_link['href']), headers=headers, timeout=10).text.lower()
                    logging.info(f"   [INFO] Checking '{hint}' page for location..."); break
                except requests.exceptions.RequestException: continue
        if re.search(r'gstin\s*[:\-]?\s*[0-9A-Z]{15}', page_html_to_check) or \
           re.search(r'\b(pincode|pin code|pin)[\s:-]*\d{6}\b', page_html_to_check) or \
           any(keyword in page_html_to_check for keyword in INDIAN_TECH_KEYWORDS) or \
           '.in' in urlparse(url).netloc:
            score += 1; logging.info("   [PASS] High-confidence Indian location signal found.")
        elif "india" in page_html_to_check:
            score += 0.5; logging.info("   [INFO] Mentions India in content.")
        if score < 1.5:
            logging.warning(f"   [FAIL] Final score {score}/2 is too low."); return None
        logging.info(f"   ✅ Valid Indian e-commerce lead found (Score: {score}/2): {url}")
        return {"URL": url, "Email": _extract_email(page_html_to_check), "Phone Number": _extract_phone_number(response.text), "social_links": _extract_social_links(soup)}
    except requests.exceptions.RequestException as e:
        logging.warning(f"   [FAIL] Could not access the site: {e}"); return None
    except Exception as e:
        logging.error(f"   [FAIL] An unexpected error during analysis: {e}"); return None


# --- MAIN EXECUTION BLOCK (MODIFIED) ---
if __name__ == "__main__":
    logging.info("🚀 Starting E-commerce Site Discovery Tool...")

    if not SERPER_API_KEY:
        logging.error("❌ SERPER_API_KEY environment variable not set. Exiting.")
        exit(1)

    # MODIFIED: Get both worksheets
    leads_worksheet, logs_worksheet = setup_google_sheet()
    if not leads_worksheet or not logs_worksheet:
        exit(1)

    existing_urls = get_existing_urls_from_sheet(leads_worksheet)
    all_potential_urls = set()

    try:
        with open(SEARCH_CONFIG_FILE, 'r') as f:
            search_data = json.load(f)
    except FileNotFoundError:
        logging.error(f"❌ Search config file not found at '{SEARCH_CONFIG_FILE}'. Exiting.")
        exit(1)

    # 1. Discover potential URLs from all phrases first
    for category, phrases in search_data.items():
        logging.info(f"\n--- Searching {len(phrases)} phrases in category: {category} ---")
        for phrase in phrases:
            logging.info(f"🔍 Searching API: \"{phrase}\"")
            api_url = "https://google.serper.dev/search"
            payload = json.dumps({"q": phrase, "gl": "in", "num": 100})
            headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
            try:
                response = requests.post(api_url, headers=headers, data=payload, timeout=10)
                results = response.json().get('organic', [])
                for result in results:
                    url = result.get('link')
                    if url:
                        cleaned_url = clean_and_validate_url(url)
                        if cleaned_url:
                            all_potential_urls.add(cleaned_url)
            except Exception as e:
                logging.error(f"❌ Error during API search for '{phrase}': {e}")

    # 2. Filter out URLs that are already in the Google Sheet
    new_urls_to_check = list(all_potential_urls - existing_urls)
    random.shuffle(new_urls_to_check)
    logging.info(f"\n--- Found {len(all_potential_urls)} potential sites. "
                 f"Analyzing {len(new_urls_to_check)} new sites. ---")

    # 3. Analyze new URLs in parallel and count successes
    successful_leads_count = 0
    if new_urls_to_check:
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_url = {executor.submit(analyze_site, url): url for url in new_urls_to_check}
            for future in as_completed(future_to_url):
                try:
                    lead_data = future.result()
                    if lead_data:
                        save_to_gsheet(leads_worksheet, lead_data)
                        successful_leads_count += 1 # Increment counter
                except Exception as e:
                    logging.error(f"Error processing a future: {e}")

    # 4. Save the final summary log to the Google Sheet
    final_summary = f"📈 Total new leads found in this run: {successful_leads_count}"
    logging.info("\n" + "="*40)
    logging.info(final_summary)
    logging.info("🎉 Discovery complete!")
    logging.info("="*40 + "\n")

    save_log_to_gsheet(logs_worksheet, successful_leads_count, final_summary)
