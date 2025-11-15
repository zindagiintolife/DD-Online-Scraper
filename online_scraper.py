#!/usr/bin/env python3
"""
DamaDam Online Profile Scraper v4.0 - OPTIMIZED SINGLE FILE
Fixes: Duplicate entries, Dashboard formatting, Online tracking
"""

import os
import sys
import time
import random
import re
import json
import pickle
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ============================================================
# PACKAGE VERIFICATION
# ============================================================
required_packages = ['selenium', 'gspread', 'google.auth']
missing = []
for pkg in required_packages:
    try:
        __import__(pkg)
    except ImportError:
        missing.append(pkg)

if missing:
    print(f"❌ Missing: {', '.join(missing)}")
    print(f"Install: pip install {' '.join(missing)}")
    sys.exit(1)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIGURATION
# ============================================================
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

# Environment Variables
USERNAME = os.getenv('DAMADAM_USERNAME')
PASSWORD = os.getenv('DAMADAM_PASSWORD')
USERNAME_2 = os.getenv('DAMADAM_USERNAME_2', '')
PASSWORD_2 = os.getenv('DAMADAM_PASSWORD_2', '')
SHEET_URL = os.getenv('GOOGLE_SHEET_URL')
GOOGLE_CREDS = os.getenv('GOOGLE_CREDENTIALS_JSON', '')

# Settings
MAX_PROFILES = int(os.getenv('MAX_PROFILES_PER_RUN', '0'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '20'))
MIN_DELAY = float(os.getenv('MIN_DELAY', '0.4'))
MAX_DELAY = float(os.getenv('MAX_DELAY', '0.6'))
PAGE_TIMEOUT = int(os.getenv('PAGE_LOAD_TIMEOUT', '30'))
SHEET_DELAY = float(os.getenv('SHEET_WRITE_DELAY', '0.8'))

# Constants
LOGIN_URL = "https://damadam.pk/login/"
COOKIE_FILE = "damadam_cookies.pkl"
COLUMN_ORDER = [
    "IMAGE", "NICK NAME", "TAGS", "LAST POST", "LAST POST TIME", "FRIEND", "CITY",
    "GENDER", "MARRIED", "AGE", "JOINED", "FOLLOWERS", "STATUS",
    "POSTS", "PROFILE LINK", "INTRO", "SOURCE", "DATETIME SCRAP"
]
COLUMN_MAP = {name: idx for idx, name in enumerate(COLUMN_ORDER)}

# Validate environment
required_env = ['DAMADAM_USERNAME', 'DAMADAM_PASSWORD', 'GOOGLE_SHEET_URL', 'GOOGLE_CREDENTIALS_JSON']
missing_vars = [v for v in required_env if not os.getenv(v)]
if missing_vars:
    print(f"❌ Missing env vars: {', '.join(missing_vars)}")
    sys.exit(1)

# ============================================================
# HELPER FUNCTIONS
# ============================================================
def get_pkt_time():
    """Pakistan time (UTC+5)"""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=5)

def log(msg):
    """Log with timestamp"""
    print(f"  [{get_pkt_time().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

def clean_text(text):
    """Clean text"""
    if not text:
        return ""
    text = str(text).strip().replace('\xa0', ' ').replace('\n', ' ')
    return re.sub(r'\s+', ' ', text).strip()

def clean_data(value):
    """Remove unwanted values"""
    if not value:
        return ""
    value = str(value).strip()
    remove = ["No city", "Not set", "[No Posts]", "N/A", "no city", "not set", 
              "[no posts]", "n/a", "[No Post URL]", "[Error]", "no set", "none", "null", "no age"]
    return "" if value in remove else value

def convert_date(relative_text):
    """Convert '2 months ago' to 'dd-mmm-yy'"""
    if not relative_text:
        return ""
    
    text = relative_text.lower().strip()
    now = get_pkt_time()
    
    try:
        # Normalize
        abbrev = {r"\bsecs?\b": "seconds", r"\bmins?\b": "minutes", r"\bhrs?\b": "hours",
                  r"\bwks?\b": "weeks", r"\byrs?\b": "years", r"\bmon(s)?\b": "months"}
        for pat, repl in abbrev.items():
            text = re.sub(pat, repl, text)
        
        # Special cases
        if text in {"just now", "now"}:
            return now.strftime("%d-%b-%y")
        if text == "yesterday":
            return (now - timedelta(days=1)).strftime("%d-%b-%y")
        
        # Parse 'a/an <unit> ago'
        aa = re.search(r"\b(a|an)\s+(second|minute|hour|day|week|month|year)s?\s*ago\b", text)
        if aa:
            amount, unit = 1, aa.group(2)
        else:
            match = re.search(r"(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*ago", text)
            if not match:
                return relative_text
            amount, unit = int(match.group(1)), match.group(2)
        
        deltas = {'second': timedelta(seconds=amount), 'minute': timedelta(minutes=amount),
                  'hour': timedelta(hours=amount), 'day': timedelta(days=amount),
                  'week': timedelta(weeks=amount), 'month': timedelta(days=amount*30),
                  'year': timedelta(days=amount*365)}
        
        if unit in deltas:
            return (now - deltas[unit]).strftime("%d-%b-%y")
        return relative_text
    except:
        return relative_text

def to_url(href):
    """Ensure absolute URL"""
    if not href:
        return ""
    href = href.strip()
    if href.startswith('/'):
        return f"https://damadam.pk{href}"
    elif not href.startswith('http'):
        return f"https://damadam.pk/{href}"
    return href

def col_letter(idx):
    """Convert index to Excel column (A, B, C...)"""
    result = ""
    idx += 1
    while idx > 0:
        idx -= 1
        result = chr(idx % 26 + ord('A')) + result
        idx //= 26
    return result

def calc_eta(processed, total, start):
    """Calculate ETA"""
    if processed == 0:
        return "Calculating..."
    elapsed = time.time() - start
    rate = processed / elapsed
    remaining = total - processed
    eta = remaining / rate if rate > 0 else 0
    
    if eta < 60:
        return f"{int(eta)}s"
    elif eta < 3600:
        return f"{int(eta/60)}m {int(eta%60)}s"
    else:
        return f"{int(eta/3600)}h {int((eta%3600)/60)}m"

# ============================================================
# BROWSER SETUP
# ============================================================
def setup_browser():
    """Initialize Chrome browser"""
    try:
        log("🔧 Setting up browser...")
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--log-level=3")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(PAGE_TIMEOUT)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        log("✅ Browser ready")
        return driver
    except Exception as e:
        log(f"❌ Browser setup failed: {e}")
        return None

def restart_browser(driver):
    """Restart crashed browser"""
    try:
        if driver:
            driver.quit()
    except:
        pass
    time.sleep(2)
    return setup_browser()

# ============================================================
# AUTHENTICATION
# ============================================================
def save_cookies(driver):
    """Save cookies"""
    try:
        with open(COOKIE_FILE, 'wb') as f:
            pickle.dump(driver.get_cookies(), f)
        return True
    except:
        return False

def load_cookies(driver):
    """Load cookies"""
    try:
        if not os.path.exists(COOKIE_FILE):
            return False
        with open(COOKIE_FILE, 'rb') as f:
            cookies = pickle.load(f)
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except:
                pass
        return True
    except:
        return False

def login_with_creds(driver, username, password, account):
    """Login attempt"""
    try:
        log(f"Trying {account}: {username}")
        
        selectors = [
            {"nick": "#nick", "pass": "#pass", "button": "form button"},
            {"nick": "input[name='nick']", "pass": "input[name='pass']", "button": "button[type='submit']"}
        ]
        
        for sel in selectors:
            try:
                nick_field = WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, sel["nick"]))
                )
                pass_field = driver.find_element(By.CSS_SELECTOR, sel["pass"])
                submit_btn = driver.find_element(By.CSS_SELECTOR, sel["button"])
                
                nick_field.clear()
                time.sleep(0.5)
                nick_field.send_keys(username)
                
                pass_field.clear()
                time.sleep(0.5)
                pass_field.send_keys(password)
                
                submit_btn.click()
                time.sleep(4)
                
                if "login" not in driver.current_url.lower():
                    log(f"✅ {account} login successful")
                    save_cookies(driver)
                    return True
                break
            except:
                continue
        return False
    except Exception as e:
        log(f"❌ {account} login error: {e}")
        return False

def login(driver):
    """Main login function"""
    try:
        log("🔐 Logging in...")
        driver.get("https://damadam.pk/")
        time.sleep(2)
        
        # Try cookies first
        if load_cookies(driver):
            driver.refresh()
            time.sleep(3)
            if "login" not in driver.current_url.lower():
                page = driver.page_source.lower()
                if any(x in page for x in ['logout', 'profile', 'settings']):
                    log("✅ Login via cookies")
                    return True
        
        # Try credentials
        driver.get(LOGIN_URL)
        time.sleep(3)
        
        if USERNAME and PASSWORD:
            if login_with_creds(driver, USERNAME, PASSWORD, "Account 1"):
                return True
        
        if USERNAME_2 and PASSWORD_2:
            driver.get(LOGIN_URL)
            time.sleep(3)
            if login_with_creds(driver, USERNAME_2, PASSWORD_2, "Account 2"):
                return True
        
        log("❌ All login attempts failed")
        return False
    except Exception as e:
        log(f"❌ Login error: {e}")
        return False

# ============================================================
# PROFILE SCRAPING
# ============================================================
def get_friend_status(driver):
    """Check friend status"""
    try:
        page = driver.page_source.lower()
        if 'action="/follow/remove/"' in page or 'unfollow.svg' in page:
            return "Yes"
        if 'follow.svg' in page and 'unfollow' not in page:
            return "No"
        return ""
    except:
        return ""

def extract_post_url(href, url_type='text'):
    """Extract clean post URL"""
    if url_type == 'text':
        match = re.search(r'/comments/text/(\d+)/', href)
        if match:
            return to_url(f"/comments/text/{match.group(1)}/").rstrip('/')
    elif url_type == 'image':
        match = re.search(r'/comments/image/(\d+)/', href)
        if match:
            return to_url(f"/content/{match.group(1)}/g/")
    return to_url(href)

def scrape_recent_post(driver, nickname):
    """Get recent post"""
    try:
        driver.get(f"https://damadam.pk/profile/public/{nickname}")
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "article.mbl")))
        
        recent = driver.find_element(By.CSS_SELECTOR, "article.mbl")
        post_data = {'LPOST': '', 'LDATE-TIME': ''}
        
        # Find post URL
        url_methods = [
            ("a[href*='/content/']", lambda h: to_url(h)),
            ("a[href*='/comments/text/']", lambda h: extract_post_url(h, 'text')),
            ("a[href*='/comments/image/']", lambda h: extract_post_url(h, 'image'))
        ]
        
        for selector, formatter in url_methods:
            try:
                link = recent.find_element(By.CSS_SELECTOR, selector)
                href = link.get_attribute('href')
                if href:
                    post_data['LPOST'] = formatter(href)
                    break
            except:
                continue
        
        # Find timestamp
        time_sels = ["span[itemprop='datePublished']", "time[itemprop='datePublished']", 
                     "span.cxs.cgy", "time"]
        for sel in time_sels:
            try:
                elem = recent.find_element(By.CSS_SELECTOR, sel)
                if elem.text.strip():
                    post_data['LDATE-TIME'] = convert_date(elem.text.strip())
                    break
            except:
                continue
        
        return post_data
    except:
        return {'LPOST': '', 'LDATE-TIME': ''}

def scrape_profile(driver, nickname):
    """Scrape complete profile"""
    url = f"https://damadam.pk/users/{nickname}/"
    try:
        log(f"📍 Scraping: {nickname}")
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "h1.cxl.clb.lsp")))
        
        page = driver.page_source
        now = get_pkt_time()
        
        data = {
            'NICK NAME': nickname,
            'DATETIME SCRAP': now.strftime("%d-%b-%y %I:%M %p"),
            'PROFILE LINK': url,
            'TAGS': '', 'CITY': '', 'GENDER': '', 'MARRIED': '', 'AGE': '', 'JOINED': '',
            'FOLLOWERS': '', 'POSTS': '', 'LAST POST': '', 'LAST POST TIME': '',
            'IMAGE': '', 'INTRO': '', 'STATUS': '', 'FRIEND': ''
        }
        
        # Status
        if 'Account suspended' in page or 'account suspended' in page.lower():
            data['STATUS'] = "Suspended"
        elif 'background:tomato' in page or 'style="background:tomato"' in page:
            data['STATUS'] = "Unverified"
        else:
            try:
                driver.find_element(By.CSS_SELECTOR, "div[style*='tomato']")
                data['STATUS'] = "Unverified"
            except:
                data['STATUS'] = "Verified"
        
        data['FRIEND'] = get_friend_status(driver)
        
        # Intro
        for sel in ["span.cl.sp.lsp.nos", "span.cl", ".ow span.nos"]:
            try:
                intro = driver.find_element(By.CSS_SELECTOR, sel)
                if intro.text.strip():
                    data['INTRO'] = clean_text(intro.text)
                    break
            except:
                pass
        
        # Profile fields
        fields = {'City:': 'CITY', 'Gender:': 'GENDER', 'Married:': 'MARRIED', 
                  'Age:': 'AGE', 'Joined:': 'JOINED'}
        for field, key in fields.items():
            try:
                elem = driver.find_element(By.XPATH, f"//b[contains(text(), '{field}')]/following-sibling::span[1]")
                value = elem.text.strip()
                if value:
                    if key == 'JOINED':
                        data[key] = convert_date(value)
                    elif key == 'GENDER':
                        data[key] = "💃" if value.lower() == 'female' else "🕺" if value.lower() == 'male' else value
                    elif key == 'MARRIED':
                        if value.lower() in ['yes', 'married']:
                            data[key] = "💍"
                        elif value.lower() in ['no', 'single', 'unmarried']:
                            data[key] = "❎"
                        else:
                            data[key] = value
                    else:
                        data[key] = clean_data(value)
            except:
                pass
        
        # Followers
        for sel in ["span.cl.sp.clb", ".cl.sp.clb"]:
            try:
                followers = driver.find_element(By.CSS_SELECTOR, sel)
                match = re.search(r'(\d+)', followers.text)
                if match:
                    data['FOLLOWERS'] = match.group(1)
                    break
            except:
                pass
        
        # Posts count
        for sel in ["a[href*='/profile/public/'] button div:first-child", "a[href*='/profile/public/'] button div"]:
            try:
                posts = driver.find_element(By.CSS_SELECTOR, sel)
                match = re.search(r'(\d+)', posts.text)
                if match:
                    data['POSTS'] = match.group(1)
                    break
            except:
                pass
        
        # Profile image
        for sel in ["img[src*='avatar-imgs']", "img[src*='avatar']", "div[style*='whitesmoke'] img[src*='cloudfront.net']"]:
            try:
                img = driver.find_element(By.CSS_SELECTOR, sel)
                src = img.get_attribute('src')
                if src and ('avatar' in src or 'cloudfront.net' in src):
                    data['IMAGE'] = src.replace('/thumbnail/', '/')
                    break
            except:
                pass
        
        # Recent post
        if data['POSTS'] and data['POSTS'] != '0':
            time.sleep(1)
            post_data = scrape_recent_post(driver, nickname)
            data['LAST POST'] = clean_data(post_data['LPOST'])
            data['LAST POST TIME'] = post_data.get('LDATE-TIME', '')
        
        log(f"✅ Done: {data['GENDER']}, {data['CITY']}, Posts: {data['POSTS']}")
        return data
    except WebDriverException:
        log(f"⚠️ Browser crashed")
        return None
    except Exception as e:
        log(f"❌ Error: {str(e)[:50]}")
        return None

# ============================================================
# GOOGLE SHEETS MANAGER
# ============================================================
class SheetsManager:
    def __init__(self):
        self.client = None
        self.profiles_sheet = None
        self.online_status_sheet = None
        self.tags_sheet = None
        self.dashboard_sheet = None
        self.tags_map = {}
        self.existing = {}
    
    def setup(self):
        """Setup Google Sheets"""
        try:
            log("📊 Connecting to Google Sheets...")
            
            # Authenticate
            scope = ["https://www.googleapis.com/auth/spreadsheets", 
                     "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS), scopes=scope)
            self.client = gspread.authorize(creds)
            
            spreadsheet = self.client.open_by_url(SHEET_URL)
            
            # Get or create sheets
            def get_sheet(name, cols=20, rows=1000):
                try:
                    return spreadsheet.worksheet(name)
                except:
                    return spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
            
            self.profiles_sheet = get_sheet("Profiles", len(COLUMN_ORDER))
            
            # Initialize headers
            if not self.profiles_sheet.get_all_values():
                self.profiles_sheet.append_row(COLUMN_ORDER)
            
            # Online Status sheet (NEW)
            self.online_status_sheet = get_sheet("Online Status", 3, 5000)
            if not self.online_status_sheet.get_all_values():
                self.online_status_sheet.append_row(["Nickname", "Status", "Timestamp"])
                self.format_sheet(self.online_status_sheet, "A1:C1")
            
            # Tags sheet
            try:
                self.tags_sheet = spreadsheet.worksheet("Tags")
                self.load_tags()
            except:
                self.tags_sheet = None
            
            # Dashboard
            self.dashboard_sheet = get_sheet("Dashboard", 8, 100)
            dashboard_data = self.dashboard_sheet.get_all_values()
            expected = ["Run#", "Timestamp", "Profiles", "Success", "Failed", "New", "Updated", "Online"]
            if not dashboard_data or dashboard_data[0] != expected:
                self.dashboard_sheet.clear()
                self.dashboard_sheet.append_row(expected)
                self.format_sheet(self.dashboard_sheet, "A1:H1")
            
            self.load_existing()
            self.format_profiles()
            
            log("✅ Sheets ready")
            return True
        except Exception as e:
            log(f"❌ Sheets setup failed: {e}")
            return False
    
    def format_sheet(self, sheet, range_name):
        """Apply formatting"""
        try:
            self.safe_update(
                sheet.format,
                range_name,
                {
                    "textFormat": {"bold": True, "fontSize": 9, "fontFamily": "Bona Nova SC"},
                    "horizontalAlignment": "CENTER",
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                }
            )
        except:
            pass
    
    def format_profiles(self):
        """Format profiles sheet"""
        try:
            self.safe_update(
                self.profiles_sheet.format,
                "A:R",
                {"textFormat": {"fontFamily": "Bona Nova SC", "fontSize": 8}}
            )
            self.format_sheet(self.profiles_sheet, "A1:R1")
        except:
            pass
    
    def load_tags(self):
        """Load tags mapping"""
        try:
            data = self.tags_sheet.get_all_values()
            if len(data) < 2:
                return
            
            headers = data[0]
            for col_idx, tag in enumerate(headers):
                if not tag.strip():
                    continue
                for row_idx in range(1, len(data)):
                    if col_idx < len(data[row_idx]):
                        nick = data[row_idx][col_idx].strip()
                        if nick:
                            nick_lower = nick.lower()
                            if nick_lower in self.tags_map:
                                self.tags_map[nick_lower] += f", {tag.strip()}"
                            else:
                                self.tags_map[nick_lower] = tag.strip()
            log(f"📋 Loaded {len(self.tags_map)} tags")
        except:
            pass
    
    def load_existing(self):
        """Load existing profiles"""
        try:
            rows = self.profiles_sheet.get_all_values()[1:]
            for idx, row in enumerate(rows, start=2):
                if row and len(row) > 1:
                    nick = row[1].strip().lower()
                    if nick:
                        self.existing[nick] = {'row': idx, 'data': row}
            log(f"📋 Loaded {len(self.existing)} profiles")
        except:
            pass
    
    def safe_update(self, func, *args, retries=3, **kwargs):
        """Safe update with retry"""
        for attempt in range(retries):
            try:
                result = func(*args, **kwargs)
                time.sleep(SHEET_DELAY)
                return result
            except Exception as e:
                if '429' in str(e) or 'quota' in str(e).lower():
                    wait = (attempt + 1) * 5
                    log(f"⏳ Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    if attempt == retries - 1:
                        log(f"❌ Update failed: {e}")
                        return None
        return None
    
    def get_online_users(self, driver):
        """Fetch online users"""
        try:
            log("🌐 Fetching online users...")
            driver.get("https://damadam.pk/online_kon/")
            time.sleep(2)
            
            nicknames = []
            items = driver.find_elements(By.CSS_SELECTOR, "li.mbl.cl.sp")
            
            for li in items:
                try:
                    nick = li.find_element(By.TAG_NAME, "b").text.strip()
                    if nick and len(nick) >= 3 and not nick.isdigit() and any(c.isalpha() for c in nick):
                        nicknames.append(nick)
                except:
                    continue
            
            # Fallback
            if not nicknames:
                links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/users/']")
                for link in links:
                    href = link.get_attribute('href')
                    if href and '/users/' in href:
                        nick = href.split('/users/')[-1].rstrip('/')
                        if nick and len(nick) >= 3 and not nick.isdigit() and any(c.isalpha() for c in nick) and nick not in nicknames:
                            nicknames.append(nick)
            
            log(f"✅ Found {len(nicknames)} online users")
            return nicknames
        except Exception as e:
            log(f"❌ Failed to get online users: {e}")
            return []
    
    def log_online_status(self, nickname):
        """Log to Online Status sheet - ALWAYS ADD NEW ROW"""
        try:
            timestamp = get_pkt_time().strftime("%d-%b-%y %I:%M %p")
            self.safe_update(
                self.online_status_sheet.append_row,
                [nickname, "Online", timestamp]
            )
        except Exception as e:
            log(f"⚠️ Online status logging failed: {e}")
    
    def apply_formulas(self, row_idx, data):
        """Apply link formulas"""
        link_cols = {"IMAGE", "LAST POST", "PROFILE LINK"}
        for col_name in link_cols:
            value = data.get(col_name)
            if not value:
                continue
            
            col_idx = COLUMN_MAP[col_name]
            cell = f"{col_letter(col_idx)}{row_idx}"
            
            if col_name == "IMAGE":
                formula = f'=IMAGE("{value}", 4, 50, 50)'
            elif col_name == "LAST POST":
                formula = f'=HYPERLINK("{value}", "Post")'
            else:
                formula = f'=HYPERLINK("{value}", "Profile")'
            
            self.safe_update(
                self.profiles_sheet.update,
                values=[[formula]],
                range_name=cell,
                value_input_option='USER_ENTERED'
            )
    
    def write_profile(self, data):
        """Write profile - FIX DUPLICATE ISSUE"""
        nickname = data.get("NICK NAME", "").strip()
        if not nickname:
            return {"status": "error", "error": "No nickname"}
        
        # Add tags
        data['TAGS'] = self.tags_map.get(nickname.lower(), "")
        
        # Prepare row
        row_values = []
        for col in COLUMN_ORDER:
            if col == "IMAGE":
                cell_value = ""  # Formula will fill this
            elif col == "PROFILE LINK":
                cell_value = "Profile" if data.get(col) else ""
            elif col == "LAST POST":
                cell_value = "Post" if data.get(col) else ""
            else:
                cell_value = clean_data(data.get(col, ""))
            row_values.append(cell_value)
        
        nickname_lower = nickname.lower()
        existing = self.existing.get(nickname_lower)
        
        if existing:
            # UPDATE EXISTING ROW (NO DUPLICATE)
            row_num = existing['row']
            old_data = existing['data']
            
            # Check for changes
            changed = []
            for idx, col in enumerate(COLUMN_ORDER):
                old_val = old_data[idx] if idx < len(old_data) else ""
                new_val = row_values[idx]
                if str(old_val).strip() != str(new_val).strip():
                    changed.append(col)
            
            if not changed:
                return {"status": "unchanged", "changed_fields": []}
            
            # Update the existing row
            range_name = f"A{row_num}:R{row_num}"
            self.safe_update(
                self.profiles_sheet.update,
                values=[row_values],
                range_name=range_name
            )
            self.apply_formulas(row_num, data)
            
            # Update cache
            self.existing[nickname_lower] = {'row': row_num, 'data': row_values}
            
            return {"status": "updated", "changed_fields": changed}
        else:
            # NEW PROFILE - APPEND
            self.safe_update(self.profiles_sheet.append_row, row_values)
            new_row = len(self.profiles_sheet.get_all_values())
            self.apply_formulas(new_row, data)
            
            # Add to cache
            self.existing[nickname_lower] = {'row': new_row, 'data': row_values}
            
            return {"status": "new", "changed_fields": list(COLUMN_ORDER)}
    
    def update_dashboard(self, metrics):
        """Update dashboard with run stats"""
        try:
            row_data = [
                metrics.get("Run Number", ""),
                metrics.get("Timestamp", ""),
                metrics.get("Profiles", 0),
                metrics.get("Success", 0),
                metrics.get("Failed", 0),
                metrics.get("New", 0),
                metrics.get("Updated", 0),
                "Online"  # NEW: Mark source as Online
            ]
            self.safe_update(self.dashboard_sheet.append_row, row_data)
            log("📊 Dashboard updated")
        except Exception as e:
            log(f"⚠️ Dashboard update failed: {e}")

# ============================================================
# MAIN EXECUTION
# ============================================================
def main():
    """Main scraper logic"""
    print("\n" + "="*60)
    print("🌐 DamaDam Online Profile Scraper v4.0")
    print("🎯 Mode: Online Users (No Limit)")
    print("⏰ Scheduled: Every 30 minutes")
    print("="*60)
    
    driver = None
    sheets = None
    success = failed = 0
    run_stats = defaultdict(int)
    start_time = time.time()
    targets = []
    
    try:
        # Setup browser
        driver = setup_browser()
        if not driver:
            raise Exception("Browser setup failed")
        
        # Login
        if not login(driver):
            raise Exception("Login failed")
        
        # Setup sheets
        sheets = SheetsManager()
        if not sheets.setup():
            raise Exception("Sheets setup failed")
        
        # Get online users
        online_nicks = sheets.get_online_users(driver)
        
        if not online_nicks:
            log("⚠️ No online users found")
            # Still log to dashboard
            if sheets:
                metrics = {
                    "Run Number": 1,
                    "Timestamp": get_pkt_time().strftime("%d-%b-%y %I:%M %p"),
                    "Profiles": 0,
                    "Success": 0,
                    "Failed": 0,
                    "New": 0,
                    "Updated": 0
                }
                sheets.update_dashboard(metrics)
            return
        
        # Limit if needed
        if MAX_PROFILES > 0:
            online_nicks = online_nicks[:MAX_PROFILES]
        
        targets = online_nicks
        print(f"\n🚀 Processing {len(targets)} online profiles...")
        print("-"*60)
        
        # Process each profile
        for i, nickname in enumerate(targets, 1):
            eta = calc_eta(i-1, len(targets), start_time)
            print(f"\n[{i}/{len(targets)}] {nickname} | ETA: {eta}")
            
            # Log to Online Status sheet (COMPLETE HISTORY)
            sheets.log_online_status(nickname)
            
            # Scrape profile
            profile = None
            for attempt in range(2):
                profile = scrape_profile(driver, nickname)
                if profile is None and attempt == 0:
                    driver = restart_browser(driver)
                    if driver and login(driver):
                        continue
                    else:
                        break
                break
            
            if profile:
                profile['SOURCE'] = 'Online'
                result = sheets.write_profile(profile)
                
                if result.get("status") in {"new", "updated", "unchanged"}:
                    success += 1
                    run_stats[result["status"]] += 1
                    
                    status_msg = result["status"].upper()
                    if result["status"] == "updated":
                        changed = result.get("changed_fields", [])
                        if changed:
                            status_msg += f" ({len(changed)} fields)"
                    log(f"✅ {nickname} - {status_msg}")
                else:
                    failed += 1
                    error = result.get("error", "Unknown")
                    log(f"❌ {nickname} - {error}")
            else:
                failed += 1
                log(f"❌ {nickname} - Scraping failed")
            
            # Batch pause
            if BATCH_SIZE > 0 and i % BATCH_SIZE == 0 and i < len(targets):
                log(f"⏸️ Batch pause ({i}/{len(targets)})")
                time.sleep(5)
            
            # Random delay
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        
        # Success summary
        print("\n" + "="*60)
        print(f"✅ Scraping Complete!")
        print(f"   ✓ Success: {success}")
        print(f"   ✗ Failed: {failed}")
        if len(targets) > 0:
            print(f"   📊 Success Rate: {(success/len(targets)*100):.1f}%")
        print(f"   ➕ New: {run_stats['new']}")
        print(f"   🔁 Updated: {run_stats['updated']}")
        print(f"   📎 Unchanged: {run_stats['unchanged']}")
        print("="*60)
    
    except KeyboardInterrupt:
        print("\n\n⚠️ INTERRUPTED BY USER")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # ALWAYS update dashboard (even on error)
        if sheets:
            try:
                metrics = {
                    "Run Number": 1,
                    "Timestamp": get_pkt_time().strftime("%d-%b-%y %I:%M %p"),
                    "Profiles": len(targets),
                    "Success": success,
                    "Failed": failed,
                    "New": run_stats.get('new', 0),
                    "Updated": run_stats.get('updated', 0)
                }
                sheets.update_dashboard(metrics)
            except Exception as e:
                log(f"⚠️ Dashboard logging failed: {e}")
        
        # Close browser
        try:
            if driver:
                driver.quit()
                print("🔒 Browser closed")
        except:
            pass
        
        print(f"🎯 Next run scheduled in 30 minutes")

if __name__ == "__main__":
    main()
