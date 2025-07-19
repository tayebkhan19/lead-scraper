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

# --- API KEY MANAGER ---
# (This section is unchanged)
class ApiKeyManager:
    def __init__(self, api_keys_str):
        self.keys = [key.strip() for key in api_keys_str.split(',') if key.strip()]
        self.current_index = 0
        if not self.keys:
            raise ValueError("No API keys provided.")
    def get_key(self):
        return self.keys[self.current_index]
    def rotate_key(self):
        self.current_index = (self.current_index + 1) % len(self.keys)
        logging.warning(f"API key limit likely reached. Rotating to next key (index {self.current_index}).")
        return self.get_key()

# --- CONFIGURATION ---
# (This section is unchanged)
SEARCH_CONFIG_FILE = "search_phrases.json"
GOOGLE_SHEET_NAME = os.getenv("GSHEET_NAME", "Scraped Leads")
GOOGLE_CREDS_FILE = "credentials.json"
SERPER_API_KEYS_STR = os.getenv("SERPER_API_KEYS")
USER_AGENTS = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36']
BLACKLISTED_DOMAINS = ['amazon.com', 'flipkart.com', 'myntra.com', 'ajio.com', 'meesho.com', 'nykaa.com']
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
        
        leads_worksheet = spreadsheet.sheet1
        if not leads_worksheet.get_all_values():
            leads_worksheet.append_row([
                "URL", "Email", "Phone Number", "Facebook", "Instagram",
                "Twitter", "LinkedIn", "Scraped Timestamp"
            ])
            
        try:
            logs_worksheet = spreadsheet.worksheet("Run Logs")
            # Ensure the header is correct
            if logs_worksheet.get("A1").first() != "Timestamp":
                 logs_worksheet.insert_row(["Timestamp", "Total Leads Found", "Credits Used", "Summary"], 1)
        except gspread.WorksheetNotFound:
            logs_worksheet = spreadsheet.add_worksheet(title="Run Logs", rows="100", cols="20")
            # MODIFIED: Added "Credits Used" column
            logs_worksheet.append_row(["Timestamp", "Total Leads Found", "Credits Used", "Summary"])

        logging.info("Google Sheets connection successful.")
        return leads_worksheet, logs_worksheet

    except Exception as e:
        logging.error(f"❌ Google Sheets Error: {e}")
        return None, None

def save_log_to_gsheet(worksheet, total_leads, credits_used, summary_lines):
    """Saves a summary of the run to the 'Run Logs' worksheet."""
    try:
        logging.info("Saving run summary to the log sheet...")
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        summary_text = "\n".join(summary_lines)
        # MODIFIED: Added credits_used to the row
        worksheet.append_row([timestamp, total_leads, credits_used, summary_text])
        logging.info("✅  Run summary saved successfully.")
    except Exception as e:
        logging.error(f"❌ Could not save run summary to Google Sheet: {e}")

# ... (The other utility functions like get_existing_urls_from_sheet, save_to_gsheet, etc., are unchanged) ...
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

# ... (Data extraction, analysis, and search functions are unchanged) ...
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

def get_search_results(phrase, key_manager):
    api_url = "https://google.serper.dev/search"
    payload = json.dumps({"q": phrase, "gl": "in", "num": 100})
    for _ in range(len(key_manager.keys)):
        headers = {'X-API-KEY': key_manager.get_key(), 'Content-Type': 'application/json'}
        try:
            response = requests.post(api_url, headers=headers, data=payload, timeout=10)
            if response.status_code == 403:
                key_manager.rotate_key(); continue
            response.raise_for_status()
            return response.json().get('organic', [])
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ RequestException for '{phrase}': {e}"); return []
    logging.error(f"❌ All API keys failed for '{phrase}'."); return []


# --- MAIN EXECUTION BLOCK (MODIFIED FOR CREDIT TRACKING) ---
if __name__ == "__main__":
    logging.info("🚀 Starting E-commerce Site Discovery Tool...")
    CREDITS_PER_SEARCH = 2 # Based on your plan: 100 results = 2 credits

    if not SERPER_API_KEYS_STR:
        logging.error("❌ SERPER_API_KEYS environment variable not set. Exiting.")
        exit(1)

    try:
        api_key_manager = ApiKeyManager(SERPER_API_KEYS_STR)
    except ValueError as e:
        logging.error(f"❌ {e}"); exit(1)

    leads_worksheet, logs_worksheet = setup_google_sheet()
    if not leads_worksheet or not logs_worksheet:
        exit(1)

    existing_urls = get_existing_urls_from_sheet(leads_worksheet)
    total_leads_found_in_run = 0
    log_summary_lines = []
    api_calls_made = 0 # NEW: Counter for API calls

    try:
        with open(SEARCH_CONFIG_FILE, 'r') as f:
            search_data = json.load(f)
    except FileNotFoundError:
        logging.error(f"❌ Search config file not found at '{SEARCH_CONFIG_FILE}'. Exiting.")
        exit(1)

    for category, phrases in search_data.items():
        logging.info(f"\n--- Searching {len(phrases)} phrases in category: {category} ---")
        for phrase in phrases:
            logging.info(f"\n🔍 Searching API: \"{phrase}\"")
            
            results = get_search_results(phrase, api_key_manager)
            api_calls_made += 1 # Increment counter after each search
            
            potential_urls = {clean_and_validate_url(res.get('link')) for res in results if res.get('link')}
            new_urls_to_check = list(potential_urls - existing_urls)
            
            if not new_urls_to_check:
                logging.info("   No new websites found for this phrase.")
                continue

            logging.info(f"   Found {len(new_urls_to_check)} new sites to analyze for this phrase.")
            leads_found_this_phrase = 0
            
            with ThreadPoolExecutor(max_workers=8) as executor:
                future_to_url = {executor.submit(analyze_site, url): url for url in new_urls_to_check}
                for future in as_completed(future_to_url):
                    try:
                        lead_data = future.result()
                        if lead_data:
                            leads_found_this_phrase += 1
                            save_to_gsheet(leads_worksheet, lead_data)
                            existing_urls.add(lead_data["URL"])
                    except Exception as e:
                        logging.error(f"Error processing a future: {e}")
            
            summary_line = f"📊 {leads_found_this_phrase} leads for: \"{phrase}\""
            logging.info(summary_line)
            log_summary_lines.append(summary_line)
            total_leads_found_in_run += leads_found_this_phrase

    # Calculate final credit usage
    credits_used = api_calls_made * CREDITS_PER_SEARCH
    
    final_summary_line = f"📈 Total new leads found: {total_leads_found_in_run}"
    credit_summary_line = f"💳 API Credits Used: {credits_used} ({api_calls_made} searches)"
    
    log_summary_lines.append(final_summary_line)
    log_summary_lines.append(credit_summary_line)

    logging.info("\n" + "="*40)
    logging.info(final_summary_line)
    logging.info(credit_summary_line)
    logging.info("🎉 Discovery complete!")
    logging.info("="*40 + "\n")

    # Save the collected summary to the "Run Logs" sheet
    save_log_to_gsheet(logs_worksheet, total_leads_found_in_run, credits_used, log_summary_lines)
