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
MAX_REVIEWS = 100            # Production review sample size
MODEL_NAME = "gemini-2.5-flash-lite"
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"
LOG_FILE = "pipeline_execution.log"
AI_DELAY = 0.5               # Tier 1 (300 RPM) speed

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
        print(f"📸 Debug screenshot saved: {path}")

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
        """Fixed Login: Uses text-based targeting and force-click to bypass visibility errors."""
        if not P4N_USER or not P4N_PASS: return
        print(f"🔐 Attempting Login for {P4N_USER}...")
        try:
            await page.goto("https://park4night.com/en", wait_until="networkidle")
            try: await page.click(".cc-btn-accept", timeout=3000)
            except: pass
            
            # 1. Open the Account Dropdown
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(1) # Wait for CSS transition
            
            # 2. Target 'Login' specifically within the visible dropdown list
            # Using force=True ensures we click even if the browser thinks it's hidden
            login_btn = page.locator(".pageHeader-account-dropdown >> text='Login'")
            await login_btn.click(force=True)

            # 3. Fill the sign-in modal
            await page.wait_for_selector("#signinUserId", state="visible", timeout=10000)
            await page.fill("#signinUserId", P4N_USER)
            await page.fill("#signinPassword", P4N_PASS)
            await page.keyboard.press("Enter")
            
            # 4. Verification
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(3)
            user_span = page.locator(".pageHeader-account-button span")
            actual_username = await user_span.inner_text()
            
            if P4N_USER.lower() in actual_username.lower():
                print(f"✅ Success! Logged in as: {actual_username}")
                PipelineLogger.log_event("LOGIN_SUCCESS", {"user": actual_username})
            else:
                print(f"⚠️ Verification Failed. Found: '{actual_username}'")
                await PipelineLogger.save_screenshot(page, "login_verify_failed")
                PipelineLogger.log_event("LOGIN_WARNING", {"expected": P4N_USER, "found": actual_username})

        except Exception as e:
            print(f"❌ Login UI Error: {e}")
            await PipelineLogger.save_screenshot(page, "login_error")
            PipelineLogger.log_event("LOGIN_ERROR", {"error": str(e)})

    async def analyze_with_ai(self, raw_data):
        """Analyzes property with full prompt logging."""
        system_instr = "Normalize costs to EUR and summarize reviews to English pros/cons."
        prompt = f"Analyze property data. Return JSON only:\n{json.dumps(raw_data)}"
        
        # LOG REQUEST PROMPT
        PipelineLogger.log_event("AI_REQUEST", {
            "p4n_id": raw_data.get("p4n_id"),
            "full_prompt": prompt
        })

        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1,
            system_instruction=system_instr
        )

        try:
            await asyncio.sleep(AI_DELAY)
            response = await client.aio.models.generate_content(
                model=MODEL_NAME, contents=prompt, config=config
            )
            # LOG RESPONSE
            PipelineLogger.log_event("AI_RESPONSE", {"res": response.text})
            return json.loads(response.text)
        except Exception as e:
            PipelineLogger.log_event("AI_ERROR", {"err": str(e)})
            return {}

    async def extract_atomic(self, page, url):
        print(f"📄 Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(2, 4))
            
            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            
            review_els = await page.locator(".place-feedback-article-content").all()
            raw_payload = {
                "p4n_id": p_id,
                "parking_cost": await self._get_dl(page, "Parking cost"),
                "reviews": [await r.inner_text() for r in review_els[:self.current_max_reviews]]
            }

            ai_data = await self.analyze_with_ai(raw_payload)

            row = {
                "p4n_id": p_id, "title": title, "url": url,
                "parking_min_eur": ai_data.get("parking_min", 0),
                "ai_pros": ai_data.get("pros", "N/A"),
                "ai_cons": ai_data.get("cons", "N/A"),
                "last_scraped": datetime.now()
            }
            PipelineLogger.log_event("ROW_PREPARED", row)
            self.processed_batch.append(row)
        except Exception as e: print(f"⚠️ Error {url}: {e}")

    async def _get_dl(self, page, label):
        try: return (await page.locator(f"dt:has-text('{label}') + dd").first.inner_text()).strip()
        except: return "N/A"

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0...")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            await self.login(page)

            # Discovery Phase
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="networkidle")
                    links = await page.locator("a[href*='/place/']").all()
                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            self.discovery_links.append(f"https://park4night.com{href}" if href.startswith("/") else href)
                        if self.is_dev and len(self.discovery_links) >= 1: break
                except: pass
                if self.is_dev and len(self.discovery_links) >= 1: break

            # Queue Filter
            queue = []
            for link in list(set(self.discovery_links)):
                p_id_match = re.search(r'/place/(\d+)', link)
                if not p_id_match: continue
                p_id = p_id_match.group(1)
                
                if self.is_dev:
                    queue.append(link)
                    break
