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
MAX_REVIEWS = 100            
MODEL_NAME = "gemini-2.5-flash-lite"
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"
LOG_FILE = "pipeline_execution.log"
AI_DELAY = 0.5               

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
        log_entry = {"timestamp": datetime.now().isoformat(), "type": event_type, "content": data}
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")

    @staticmethod
    async def save_screenshot(page, name):
        path = f"debug_{name}_{datetime.now().strftime('%H%M%S')}.png"
        await page.screenshot(path=path)
        print(f"📸 DEBUG: Screenshot saved: {path}")

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
        """Checks login status on current page and logs in if needed."""
        print(f"🔍 DEBUG: Checking login status on {page.url}")
        user_span = page.locator(".pageHeader-account-button span")
        
        try:
            current_text = await user_span.inner_text(timeout=5000)
            if P4N_USER and P4N_USER.lower() in current_text.lower():
                print(f"✅ DEBUG: Verified login as {current_text}")
                return
        except Exception:
            pass

        print(f"🔐 DEBUG: User not logged in. Triggering Modal...")
        try:
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(1)
            await page.click(".pageHeader-account-dropdown >> text='Login'", force=True)

            await page.wait_for_selector("#signinUserId", state="visible", timeout=10000)
            await page.fill("#signinUserId", P4N_USER)
            await page.fill("#signinPassword", P4N_PASS)
            await page.keyboard.press("Enter")
            
            await page.wait_for_selector(f".pageHeader-account-button:has-text('{P4N_USER}')", timeout=12000)
            print("✅ DEBUG: Login Successful.")
            PipelineLogger.log_event("LOGIN_SUCCESS", {"user": P4N_USER})
        except Exception as e:
            print(f"❌ DEBUG: Login Failed: {e}")
            await PipelineLogger.save_screenshot(page, "login_failed")
            PipelineLogger.log_event("LOGIN_ERROR", {"error": str(e)})

    async def analyze_with_ai(self, raw_data):
        prompt = f"Analyze property data. Return JSON only:\n{json.dumps(raw_data)}"
        PipelineLogger.log_event("AI_REQUEST", {"p4n_id": raw_data.get("p4n_id"), "prompt": prompt})

        try:
            await asyncio.sleep(AI_DELAY)
            response = await client.aio.models.generate_content(
                model=MODEL_NAME, 
                contents=prompt, 
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            PipelineLogger.log_event("AI_RESPONSE", {"res": response.text})
            return json.loads(response.text)
        except Exception as e:
            PipelineLogger.log_event("AI_ERROR", {"err": str(e)})
            return {}

    async def extract_atomic(self, page, url):
        print(f"📄 DEBUG: Starting extraction for {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded")
            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            
            review_els = await page.locator(".place-feedback-article-content").all()
            raw_payload = {
                "p4n_id": p_id,
                "reviews": [await r.inner_text() for r in review_els[:self.current_max_reviews]]
            }
            ai_data = await self.analyze_with_ai(raw_payload)
            self.processed_batch.append({
                "p4n_id": p_id, "title": title, "url": url,
                "parking_min_eur": ai_data.get("parking_min", 0),
                "ai_pros": ai_data.get("pros", "N/A"),
                "last_scraped": datetime.now()
            })
        except Exception as e: print(f"⚠️ DEBUG: Extraction Error: {e}")

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            # --- DEBUG DISCOVERY PHASE ---
            for url in TARGET_URLS:
                print(f"🔍 DEBUG: Visiting Search URL: {url}")
                await page.goto(url, wait_until="networkidle")
                
                # Close cookies if they appear
                try: await page.click(".cc-btn-accept", timeout=3000)
                except: pass

                await self.ensure_logged_in(page)

                # Count links
                links = await page.locator("a[href*='/place/']").all()
                print(f"🧪 DEBUG: Found {len(links)} links on current page.")

                if len(links) == 0:
                    print("⚠️ DEBUG: Found 0 links! Saving page state for audit.")
                    await PipelineLogger.save_screenshot(page, "discovery_0_links")
                    # Log the first 500 chars of HTML to see if it's a block page
                    html_snippet = await page.content()
                    PipelineLogger.log_event("DISCOVERY_EMPTY", {"url": url, "html_start": html_snippet[:500]})

                for link in links:
                    href = await link.get_attribute("href")
                    if href:
                        full_url = f"https://park4night.com{href}" if href.startswith("/") else href
                        self.discovery_links.append(full_url)
                    if self.is_dev and len(self.discovery_links) >= 1: break
                
                if self.is_dev and len(self.discovery_links) >= 1: break

            # --- QUEUE & PROCESS ---
            queue = list(set(self.discovery_links))
            if self.is_dev: queue = queue[:1]
            
            print(f"⚡ DEBUG: Queue has {len(queue)} items ready for AI processing.")
            for link in queue:
                await self.extract_atomic(page, link)
            
            await browser.close()
            self._upsert_and_save()

    def _upsert_and_save(self):
        if not self.processed_batch:
            print("🏁 DEBUG: No records processed. Exiting.")
            return
        new_df = pd.DataFrame(self.processed_batch)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df['last_scraped'] = pd.to_datetime(final_df['last_scraped'])
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(self.csv_file, index=False)
        print(f"🚀 Success! Updated {self.csv_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev).start())
