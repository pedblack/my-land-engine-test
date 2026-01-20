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
MAX_REVIEWS = 100            # Reviews per property for analysis
MODEL_NAME = "gemini-2.5-flash-lite" 
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"
LOG_FILE = "pipeline_execution.log"
AI_DELAY = 0.5               # Tier 1 speed (300 RPM)

# --- SYSTEM SETTINGS ---
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME") 
P4N_PASS = os.environ.get("P4N_PASSWORD") 

TARGET_URLS = [
    "https://park4night.com/en/search?lat=37.6365&lng=-8.6385&z=10",
    "https://park4night.com/en/search?lat=37.8785&lng=-8.5686&z=10"
]

class PipelineLogger:
    @staticmethod
    def log_event(event_type, data):
        """Timestamped JSON logging for audit trails."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "content": data
        }
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")

    @staticmethod
    async def save_screenshot(page, name):
        path = f"debug_{name}_{datetime.now().strftime('%H%M%S')}.png"
        await page.screenshot(path=path)
        print(f"📸 DEBUG: Screenshot saved: {path}")

client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self, is_dev=False):
        self.is_dev = is_dev
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.current_max_reviews = 1 if is_dev else MAX_REVIEWS 
        self.processed_batch = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except: pass
        return pd.DataFrame()

    async def login(self, page):
        """Targeted login using modal-footer submit button."""
        if not P4N_USER or not P4N_PASS: return
        print(f"🔐 Attempting Login for {P4N_USER}...")
        try:
            # 1. Open Dropdown & Click Login
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(1)
            # Use specific dropdown selector
            await page.click(".pageHeader-account-dropdown >> text='Login'", force=True)

            # 2. Fill Modal Inputs
            await page.wait_for_selector("#signinUserId", state="visible")
            await page.fill("#signinUserId", P4N_USER)
            await page.fill("#signinPassword", P4N_PASS)
            
            # 3. Explicitly click the Submit button in the footer
            # Based on provided HTML: .modal-footer .btn-primary
            submit_btn = page.locator(".modal-footer .btn-primary:has-text('Login')")
            await submit_btn.click()
            
            # 4. Verification
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(3)
            user_span = page.locator(".pageHeader-account-button span")
            actual_username = await user_span.inner_text()
            
            if P4N_USER.lower() in actual_username.lower():
                print(f"✅ Logged in as: {actual_username}")
                PipelineLogger.log_event("LOGIN_SUCCESS", {"user": actual_username})
            else:
                print(f"⚠️ Verification Failed. Found: '{actual_username}'")
                await PipelineLogger.save_screenshot(page, "login_failed")
        except Exception as e:
            print(f"❌ Login UI Error: {e}")
            await PipelineLogger.save_screenshot(page, "login_error")
            PipelineLogger.log_event("LOGIN_ERROR", {"error": str(e)})

    async def analyze_with_ai(self, raw_data):
        """Atomic AI call with detailed Request/Response logging."""
        prompt = f"Analyze property data. Return JSON only:\n{json.dumps(raw_data)}"
        
        # LOG LLM REQUEST
        PipelineLogger.log_event("AI_REQUEST_PROMPT", {
            "p4n_id": raw_data.get("p4n_id"),
            "prompt": prompt
        })

        config = types.GenerateContentConfig(response_mime_type="application/json", temperature=0.1)
        try:
            await asyncio.sleep(AI_DELAY) 
            response = await client.aio.models.generate_content(model=MODEL_NAME, contents=prompt, config=config)
            
            # LOG LLM RESPONSE
            PipelineLogger.log_event("AI_RESPONSE_RAW", {"res": response.text})
