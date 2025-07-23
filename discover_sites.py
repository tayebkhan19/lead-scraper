"""
A scalable script that discovers Indian e-commerce websites using a
manual phrase system, analyzes them in parallel, and saves the results.
"""
# --- IMPORTS ---
import json, os, random, re, time
from urllib.parse import urljoin, urlparse, urlunparse
import gspread, requests, phonenumbers
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='lead_discovery.log', filemode='w')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# --- CONFIGURATION ---
SEARCH_CONFIG_FILE = "search_phrases.json"
GOOGLE_SHEET_NAME = os.getenv("GSHEET_NAME", "Scraped Leads")
GOOGLE_CREDS_FILE = "credentials.json"
SERPER_API_KEY = os.getenv("SERPER_API_KEY") # Uses a single API key
USER_AGENTS = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36']
BLACKLISTED_DOMAINS = ['amazon.com', 'flipkart.com', 'myntra.com', 'ajio.com', 'meesho.com', 'nykaa.com']
NEGATIVE_PATH_KEYWORDS = ['blog', 'news', 'docs', 'forum', 'support', 'publication', 'careers']
NEGATIVE_CONTENT_KEYWORDS = ['market research', 'consulting firm']
INDIAN_TECH_KEYWORDS = ['razorpay', 'payu', 'instamojo', 'shiprocket', 'delhivery']
POLICY_PAGE_HINTS = ['contact', 'about', 'legal', 'policy', 'shipping', 'terms']
SOCIAL_MEDIA_DOMAINS = ['facebook.com', 'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com']

# --- SETUP, UTILITY, & SAVING FUNCTIONS ---
def setup_google_sheet():
    try:
        logging.info("Connecting to Google Sheets...")
        gc = gspread.service_account(filename=GOOGLE_CREDS_FILE)
        spreadsheet = gc.open(GOOGLE_SHEET_NAME)
        leads_ws = spreadsheet.sheet1
        if not leads_ws.get_all_values(): leads_ws.append_row(["URL", "Email", "Phone Number", "Facebook", "Instagram", "Twitter", "LinkedIn", "Scraped Timestamp"])
        try:
            logs_ws = spreadsheet.worksheet("Run Logs")
            if logs_ws.get("A1").first() != "Timestamp": logs_ws.insert_row(["Timestamp", "Total Leads Found", "Credits Used", "Summary"], 1)
        except gspread.WorksheetNotFound:
            logs_ws = spreadsheet.add_worksheet(title="Run Logs", rows="100", cols="20")
            logs_ws.append_row(["Timestamp", "Total Leads Found", "Credits Used", "Summary"])
        logging.info("Google Sheets connection successful.")
        return leads_ws, logs_ws
    except Exception as e: logging.error(f"❌ Google Sheets Error: {e}"); return None, None

def get_existing_urls_from_sheet(worksheet):
    try:
        logging.info("Fetching existing URLs from Google Sheet...")
        urls = worksheet.col_values(1)
        logging.info(f"Found {len(urls) -1} existing URLs in the sheet.")
        return set(urls[1:])
    except Exception as e: logging.error(f"❌ Could not fetch existing URLs: {e}"); return set()

def clean_and_validate_url(url):
    try:
        match = re.search(r'https?://[^\s?#]+', url)
        if not match: return None
        url_to_parse = match.group(0)
        if any(f"/{keyword}" in url_to_parse for keyword in NEGATIVE_PATH_KEYWORDS): return None
        parsed = urlparse(url_to_parse)
        cleaned_url, domain = urlunparse((parsed.scheme, parsed.netloc, '', '', '', '')), parsed.netloc.replace('www.', '')
        if any(blacklisted in domain for blacklisted in BLACKLISTED_DOMAINS): return None
        return cleaned_url
    except Exception: return None

def save_to_gsheet(worksheet, lead_data):
    try:
        timestamp, socials = time.strftime("%Y-%m-%d %H:%M:%S"), lead_data.get("social_links", {})
        worksheet.append_row([lead_data.get("URL", "N/F"), lead_data.get("Email", "N/F"), lead_data.get("Phone Number", "N/F"), socials.get("facebook", "N/F"), socials.get("instagram", "N/F"), socials.get("twitter", "N/F"), socials.get("linkedin", "N/F"), timestamp])
        logging.info(f"✅ Saved to Google Sheet: {lead_data.get('URL')}")
    except gspread.exceptions.APIError as e: logging.error(f"❌ GSheet API error: {e}"); time.sleep(60)

def save_log_to_gsheet(worksheet, total_leads, credits_used, summary_lines):
    try:
        logging.info("Saving run summary to the log sheet...")
        timestamp, summary_text = time.strftime("%Y-%m-%d %H:%M:%S"), "\n".join(summary_lines)
        worksheet.append_row([timestamp, total_leads, credits_used, summary_text])
        logging.info("✅ Run summary saved successfully.")
    except Exception as e: logging.error(f"❌ Could not save run summary: {e}")

# --- DATA EXTRACTION & ANALYSIS FUNCTIONS ---
def _extract_email(text):
    emails = re.findall(r'[\w\.\-]+@[\w\.\-]+\.\w+', text)
    if emails: return "\n".join(list(set(emails)))
    return "Not Found"

def _extract_phone_number(text):
    found_numbers = []
    for match in phonenumbers.PhoneNumberMatcher(text, "IN"):
        formatted_number = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
        if formatted_number not in found_numbers: found_numbers.append(formatted_number)
    if found_numbers: return "\n".join(found_numbers)
    return "Not Found"

def _extract_social_links(soup):
    social_links = {}
    for a in soup.find_all('a', href=True):
        href = a['href']
        for domain in SOCIAL_MEDIA_DOMAINS:
            platform = domain.split('.')[0]
            if domain in href and platform not in social_links: social_links[platform] = href
    return social_links

def analyze_site(url):
    logging.info(f"   Analyzing {url}...")
    try:
        time.sleep(random.uniform(1.0, 3.0))
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        response = requests.get(url, headers=headers, timeout=15, verify=True)
        response.raise_for_status()
        if not response.text: logging.warning(f"   [FAIL] Site returned empty content: {url}"); return None
        html_text, soup, score = response.text.lower(), BeautifulSoup(response.text, 'html.parser'), 0
        if any(soup.find(['button', 'a', 'input'], text=re.compile(r'add to cart|buy now|shop now', re.I))) or \
           len(soup.find_all(attrs={'class': re.compile(r'product|item|grid|listing', re.I)})) >= 3 or \
           '"@type":"product"' in html_text or any(tag in html_text for tag in ['shopify', 'woocommerce']): score += 1
        else: logging.info("   [FAIL] No strong e-commerce signals found."); return None
        if any(keyword in html_text for keyword in NEGATIVE_CONTENT_KEYWORDS): logging.warning("   [FAIL] Contains blacklisted content keyword."); return None
        page_html_to_check = html_text
        for hint in POLICY_PAGE_HINTS:
            page_link = soup.find('a', href=re.compile(hint), text=re.compile(hint, re.I))
            if page_link and page_link.get('href'):
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    page_response = requests.get(urljoin(url, page_link['href']), headers=headers, timeout=10)
                    if page_response.text: page_html_to_check += page_response.text.lower()
                    logging.info(f"   [INFO] Checking '{hint}' page for location..."); break
                except requests.exceptions.RequestException: continue
        if re.search(r'gstin\s*[:\-]?\s*[0-9A-Z]{15}', page_html_to_check) or \
           re.search(r'\b(pincode|pin code|pin)[\s:-]*\d{6}\b', page_html_to_check) or \
           any(keyword in page_html_to_check for keyword in INDIAN_TECH_KEYWORDS) or \
           '.in' in urlparse(url).netloc: score += 1; logging.info("   [PASS] High-confidence Indian location signal found.")
        elif "india" in page_html_to_check: score += 0.5; logging.info("   [INFO] Mentions India in content.")
        if score < 1.5: logging.warning(f"   [FAIL] Final score {score}/2 is too low."); return None
        logging.info(f"   ✅ Valid Indian e-commerce lead found (Score: {score}/2): {url}")
        return {"URL": url, "Email": _extract_email(page_html_to_check), "Phone Number": _extract_phone_number(response.text), "social_links": _extract_social_links(soup)}
    except requests.exceptions.RequestException as e: logging.warning(f"   [FAIL] Could not access site: {e}"); return None
    except Exception as e: logging.error(f"   [FAIL] Unexpected analysis error: {e}"); return None

def get_search_results(phrase_obj):
    phrase, page = phrase_obj['phrase'], phrase_obj['page']
    api_url = "https://google.serper.dev/search"
    payload = json.dumps({"q": phrase, "gl": "in", "num": 100, "page": page})
    headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
    try:
        response = requests.post(api_url, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        return response.json().get('organic', [])
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ API search failed for '{phrase}': {e}")
        return []

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    logging.info("🚀 Starting E-commerce Site Discovery Tool...")
    CREDITS_PER_SEARCH, PHRASES_PER_RUN = 2, 30
    MAX_PAGE_SEARCH = 2 # MODIFIED: Search up to page 2 only

    if not SERPER_API_KEY: logging.error("❌ SERPER_API_KEY not set."); exit(1)

    leads_ws, logs_ws = setup_google_sheet()
    if not leads_ws or not logs_ws: exit(1)

    existing_urls = get_existing_urls_from_sheet(leads_ws)
    total_leads_found, log_summary, api_calls_made = 0, [], 0
    
    try:
        with open(SEARCH_CONFIG_FILE, 'r') as f: all_phrases = json.load(f)
    except FileNotFoundError: logging.error(f"❌ '{SEARCH_CONFIG_FILE}' not found."); exit(1)

    phrases_to_process = all_phrases[:PHRASES_PER_RUN]
    remaining_phrases = all_phrases[PHRASES_PER_RUN:]

    if not phrases_to_process: logging.info("✅ No phrases left to process."); exit(0)
    
    logging.info(f"--- Processing a batch of {len(phrases_to_process)} phrases. ---")
    
    processed_phrases = []
    for phrase_obj in phrases_to_process:
        logging.info(f"\n🔍 Searching API: \"{phrase_obj['phrase']}\" (Page {phrase_obj['page']})")
        results = get_search_results(phrase_obj)
        api_calls_made += 1
        
        potential_urls = {clean_and_validate_url(res.get('link')) for res in results if res.get('link')}
        new_urls_to_check = list(potential_urls - existing_urls)
        
        leads_found_this_phrase = 0
        if new_urls_to_check:
            logging.info(f"   Found {len(new_urls_to_check)} new sites to analyze.")
            with ThreadPoolExecutor(max_workers=8) as executor:
                future_to_url = {executor.submit(analyze_site, url): url for url in new_urls_to_check}
                for future in as_completed(future_to_url):
                    try:
                        if lead_data := future.result():
                            leads_found_this_phrase += 1
                            save_to_gsheet(leads_ws, lead_data)
                            existing_urls.add(lead_data["URL"])
                    except Exception as e: logging.error(f"Error processing a future: {e}")
        
        summary_line = f"📊 {leads_found_this_phrase} leads for: \"{phrase_obj['phrase']}\" (Page {phrase_obj['page']})"
        logging.info(summary_line)
        log_summary.append(summary_line)
        total_leads_found += leads_found_this_phrase

        phrase_obj['page'] = 1 if phrase_obj['page'] >= MAX_PAGE_SEARCH else phrase_obj['page'] + 1
        processed_phrases.append(phrase_obj)
    
    final_phrase_list = remaining_phrases + processed_phrases
    with open(SEARCH_CONFIG_FILE, 'w') as f: json.dump(final_phrase_list, f, indent=2)
    logging.info(f"✅ Updated '{SEARCH_CONFIG_FILE}' and moved processed phrases to the end.")

    credits_used = api_calls_made * CREDITS_PER_SEARCH
    final_summary_line = f"📈 Total new leads: {total_leads_found}"
    credit_summary_line = f"💳 API Credits Used: {credits_used} ({api_calls_made} searches)"
    
    log_summary.insert(0, credit_summary_line)
    log_summary.insert(0, final_summary_line)

    logging.info("\n" + "="*40)
    for line in log_summary: logging.info(line)
    logging.info("🎉 Discovery complete!")
    logging.info("="*40 + "\n")

    save_log_to_gsheet(logs_ws, total_leads_found, credits_used, log_summary)
