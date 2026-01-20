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
AI_DELAY = 0.5               # Tier 1 (300 RPM) speed optimization

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
        """Appends a timestamped JSON event to the log file."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "content": data
        }
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")

    @staticmethod
    async def save_screenshot(page, name):
        """Saves a screenshot for debugging login/visibility issues."""
        path = f"debug_{name}_{datetime.now().strftime('%H%M%S')}.png"
        await page.screenshot(path=path)
        print(f"📸 Debug screenshot saved: {path}")

if not GEMINI_API_KEY:
    print("❌ ERROR: GOOGLE_API_KEY not found.")
    exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self, is_dev=False):
        self.is_dev = is_dev
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.current_max_reviews = 1 if is_dev else MAX_REVIEWS 
        self.discovery_links = []
        self.processed_batch = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except Exception: pass
        return pd.DataFrame()

    async def ensure_logged_in(self, page):
        """Checks login status and performs login directly on the search page."""
        if not P4N_USER or not P4N_PASS:
            PipelineLogger.log_event("LOGIN_SKIP", "No credentials in environment")
            return

        user_span = page.locator(".pageHeader-account-button span")
        
        # Check if already logged in (span contains username)
        try:
            current_text = await user_span.inner_text(timeout=3000)
            if P4N_USER.lower() in current_text.lower():
                print(f"✅ Already logged in as {current_text}")
                return
        except Exception:
            pass

        print(f"🔐 Not logged in. Triggering login on: {page.url}")
        try:
            # Open Dropdown and click Login
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(1) # Let animation settle
            
            login_btn = page.locator(".pageHeader-account-dropdown >> text='Login'")
            await login_btn.click(force=True)

            # Fill Modal
            await page.wait_for_selector("#signinUserId", state="visible", timeout=10000)
            await page.fill("#signinUserId", P4N_USER)
            await page.fill("#signinPassword", P4N_PASS)
            await page.keyboard.press("Enter")
            
            # Wait for header update
            await page.wait_for_selector(f".pageHeader-account-button:has-text('{P4N_USER}')", timeout=12000)
            print(f"✅ Login verified successfully.")
            PipelineLogger.log_event("LOGIN_SUCCESS", {"user": P4N_USER, "url": page.url})
        except Exception as e:
            print(f"❌ Login failed: {e}")
            await PipelineLogger.save_screenshot(page, "login_failure")
            PipelineLogger.log_event("LOGIN_ERROR", {"error": str(e)})

    async def analyze_with_ai(self, raw_data):
        """Atomic AI request with full prompt and response logging."""
        system_instr = "Normalize costs to EUR and summarize reviews to English pros/cons."
        prompt = f"Analyze property data. Return JSON only:\n{json.dumps(raw_data)}"
        
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
            PipelineLogger.log_event("AI_RESPONSE", {"res": response.text})
            return json.loads(response.text)
        except Exception as e:
            PipelineLogger.log_event("AI_ERROR", {"err": str(e)})
            return {}

    async def extract_atomic(self, page, url):
        print(f"📄 Scraping Property: {url}")
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
                "parking_max_eur": ai_data.get("parking_max", 0),
                "ai_pros": ai_data.get("pros", "N/A"),
                "ai_cons": ai_data.get("cons", "N/A"),
                "last_scraped": datetime.now()
            }
            PipelineLogger.log_event("ROW_PREPARED", row)
            self.processed_batch.append(row)
        except Exception as e:
            print(f"⚠️ Extraction Error {url}: {e}")

    async def _get_dl(self, page, label):
        try: return (await page.locator(f"dt:has-text('{label}') + dd").first.inner_text()).strip()
        except Exception: return "N/A"

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            # 1. Discovery Phase (Contextual Login)
            for url in TARGET_URLS:
                print(f"🔍 Accessing Search: {url}")
                await page.goto(url, wait_until="networkidle")
                
                try: await page.click(".cc-btn-accept", timeout=2000)
                except Exception: pass

                # Login right here on the search result page
                await self.ensure_logged_in(page)

                links = await page.locator("a[href*='/place/']").all()
                print(f"🧪 Found {len(links)} location links.")
                
                for link in links:
                    href = await link.get_attribute("href")
                    if href:
                        full_url = f"https://park4night.com{href}" if href.startswith("/") else href
                        self.discovery_links.append(full_url)
                    if self.is_dev and len(self.discovery_links) >= 1: break
                
                if self.is_dev and len(self.discovery_links) >= 1: break

            # 2. Filter Queue
            unique_links = list(set(self.discovery_links))
            queue = []
            for link in unique_links:
                p_id_match = re.search(r'/place/(\d+)', link)
                if not p_id_match: continue
                p_id = p_id_match.group(1)
                
                if self.is_dev:
                    queue.append(link)
                    break # Strictly 1 item in dev mode
                
                is_stale = True
                if not self.existing_df.empty and p_id in self.existing_df['p4n_id'].astype(str).values:
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]['last_scraped'].iloc[0]
                    if (datetime.now() - last_date) < timedelta(days=7): 
                        is_stale = False
                if is_stale: queue.append(link)

            # 3. Process
            print(f"⚡ Queue contains {len(queue)} items.")
            for link in queue:
                await self.extract_atomic(page, link)
            
            await browser.close()
            self._upsert_and_save()

    def _upsert_and_save(self):
        if not self.processed_batch:
            print("🏁 No new records to save.")
            return
        new_df = pd.DataFrame(self.processed_batch)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df['last_scraped'] = pd.to_datetime(final_df['last_scraped'])
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to
