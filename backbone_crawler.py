import asyncio
import random
import re
import os
import argparse
import json
import pandas as pd
from datetime import datetime, timedelta
from google import genai
from google.genai import types
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIGURABLE CONSTANTS ---
MODEL_NAME = "gemini-2.5-flash-lite"  # Much cheaper than flash.
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"

# --- TIER 1 QUOTA (300 RPM) ---
# Safe speed for Tier 1.
AI_DELAY = 0.5 

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME")
P4N_PASS = os.environ.get("P4N_PASSWORD")

TARGET_URLS = [
    "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10",
    "https://park4night.com/en/search?lat=37.87856774592691&lng=-8.568677272965147&z=10"
]

if not GEMINI_API_KEY:
    print("❌ ERROR: GOOGLE_API_KEY not found.")
    exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self, is_dev=False):
        self.is_dev = is_dev
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.max_reviews = 1 if is_dev else 100 # Ultra-lean in DEV
        self.discovery_links = []
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
        """Automated Login Flow using the Modal."""
        if not P4N_USER or not P4N_PASS:
            print("ℹ️ Skipping Login: Credentials missing.")
            return
        print("🔐 Opening Login Modal...")
        try:
            await page.goto("https://park4night.com/en", wait_until="networkidle")
            try: await page.click(".cc-btn-accept", timeout=3000)
            except: pass
            await page.click(".pageHeader-account-button")
            await page.click("button[data-bs-target='#signinModal']")
            await page.wait_for_selector("#signinUserId", state="visible")
            await page.fill("#signinUserId", P4N_USER)
            await page.fill("#signinPassword", P4N_PASS)
            await page.keyboard.press("Enter")
            await asyncio.sleep(5)
            print("✅ Login completed.")
        except Exception as e:
            print(f"❌ Login UI Error: {e}")

    async def analyze_with_ai(self, raw_data, retries=3):
        """Tier 1 Atomic AI call with automated retry logic for 429s."""
        prompt = f"Analyze property data. Return JSON only:\n{json.dumps(raw_data)}"
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1,
            system_instruction="Normalize costs to EUR and summarize reviews to English pros/cons."
        )

        for attempt in range(retries):
            try:
                await asyncio.sleep(AI_DELAY) 
                response = await client.aio.models.generate_content(
                    model=MODEL_NAME, contents=prompt, config=config
                )
                return json.loads(response.text)
            except Exception as e:
                if "429" in str(e) and attempt < retries - 1:
                    wait = (attempt + 1) * 10
                    print(f"⚠️ Rate limit hit. Retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                print(f"🤖 AI Failure: {e}")
                return {}

    async def extract_atomic(self, page, url):
        print(f"📄 Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(2, 4))
            
            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            review_els = await page.locator(".place-feedback-article-content").all()
            
            raw_payload = {
                "p4n_id": p_id,
                "parking_cost": await self._get_dl(page, "Parking cost"),
                "reviews": [await r.inner_text() for r in review_els[:self.max_reviews]]
            }

            ai_data = await self.analyze_with_ai(raw_payload)

            self.processed_batch.append({
                "p4n_id": p_id,
                "title": title,
                "url": url,
                "parking_min_eur": ai_data.get("parking_min", 0),
                "parking_max_eur": ai_data.get("parking_max", 0),
                "ai_pros": ai_data.get("pros", "N/A"),
                "ai_cons": ai_data.get("cons", "N/A"),
                "last_scraped": datetime.now()
            })
        except Exception as e:
            print(f"⚠️ Extraction Error {url}: {e}")

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

            # Discovery
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

            # Filter
            queue = []
            for link in list(set(self.discovery_links)):
                match = re.search(r'/place/(\d+)', link)
                if not match: continue
                p_id = match.group(1)
                
                if self.is_dev:
                    queue.append(link)
                    break # Strictly 1 property in DEV
                
                is_stale = True
                if not self.existing_df.empty and p_id in self.existing_df['p4n_id'].astype(str).values:
                    last_scrape = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]['last_scraped'].iloc[0]
                    if (datetime.now() - last_scrape) < timedelta(days=7):
                        is_stale = False
                if is_stale: queue.append(link)

            print(f"⚡ Queue: {len(queue)} items.")
            for link in queue:
                await self.extract_atomic(page, link)
            
            await browser.close()
            self._upsert_and_save()

    def _upsert_and_save(self):
        if not self.processed_batch: return
        new_df = pd.DataFrame(self.processed_batch)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df['last_scraped'] = pd.to_datetime(final_df['last_scraped'])
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(self.csv_file, index=False)
        print(f"🚀 Success! Updated {self.csv_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true', help='Run in dev mode')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev).start())
