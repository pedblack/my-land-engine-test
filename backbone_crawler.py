import asyncio
import random
import re
import os
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
from google import genai
from google.genai import types
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIGURABLE CONSTANTS ---
MODEL_NAME = "gemini-2.5-flash-lite" 
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"
LOG_FILE = "pipeline_execution.log"

AI_DELAY = 1.5               
STALENESS_DAYS = 30          
MIN_REVIEWS_THRESHOLD = 3    
DEV_LIMIT = 1                

URL_LIST_FILE = "url_list.txt"   
STATE_FILE = "queue_state.json"  

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME") 
P4N_PASS = os.environ.get("P4N_PASSWORD") 

class P4NScraper:
    def __init__(self, is_dev=False, force=False):
        self.is_dev = is_dev
        self.force = force 
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.processed_batch = []
        self.existing_df = self._load_existing()
        self.stats = {"read": 0, "discarded_fresh": 0, "discarded_low_feedback": 0, "gemini_calls": 0}

    def _load_existing(self):
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'], errors='coerce')
                return df
            except: pass
        return pd.DataFrame()

    async def login(self, page):
        if not P4N_USER or not P4N_PASS:
            print("⚠️ [LOGIN] Missing credentials.")
            return False

        print(f"🔐 [LOGIN] Attempting for user: {P4N_USER}...")
        try:
            # 1. Open Modal
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(1)
            await page.click(".pageHeader-account-dropdown >> text='Login'", force=True)
            
            # 2. Direct fill for reliability
            await page.wait_for_selector("#signinUserId", state="visible", timeout=10000)
            await page.locator("#signinUserId").fill(P4N_USER)
            await page.locator("#signinPassword").fill(P4N_PASS)
            
            # 3. Targeted Click on the Submit Button from your shared DOM
            print("⏳ [LOGIN] Clicking Submit Button...")
            submit_selector = "#signinModal .modal-footer button[type='submit']:has-text('Login')"
            await page.click(submit_selector, force=True)
            
            # 4. Wait for Modal Closure or Error
            try:
                await page.wait_for_selector("#signinModal", state="hidden", timeout=12000)
            except:
                error_msg = await page.locator("text='ID or password error'").is_visible()
                if error_msg:
                    print("❌ [LOGIN] Server rejected credentials.")
                    await page.screenshot(path=f"login_failure_{int(datetime.now().timestamp())}.png")
                    return False

            # 5. Robust Session Verification
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(5) 

            # Check for username in the nav button span as requested
            account_span = page.locator(".pageHeader-account-button span")
            await account_span.wait_for(state="visible", timeout=5000)
            found_user = (await account_span.inner_text()).strip()
            
            if P4N_USER.lower() in found_user.lower():
                print(f"✅ [LOGIN] Verified successfully: Logged in as '{found_user}'")
                await page.screenshot(path=f"login_success_{int(datetime.now().timestamp())}.png", full_page=True)
                return True
            else:
                print(f"❌ [LOGIN] Validation failed. Found: '{found_user}'")
                await page.screenshot(path=f"login_failed_final_{int(datetime.now().timestamp())}.png")
                return False

        except Exception as e: 
            print(f"❌ [LOGIN] Error: {e}")
            await page.screenshot(path=f"login_exception_{int(datetime.now().timestamp())}.png")
            return False

    # ... [Remaining methods: analyze_with_ai, extract_atomic, start, _upsert_and_save] ...

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev, force=args.force).start())
