import asyncio
import random
import re
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from google import genai
from google.genai import types
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIGURABLE CONSTANTS ---
MAX_REVIEWS = 50
BATCH_SIZE = 10
MODEL_NAME = "gemini-2.5-flash-lite" 
CSV_FILE = "backbone_locations.csv"

# --- SYSTEM SETTINGS ---
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME") # Your username secret
P4N_PASS = os.environ.get("P4N_PASSWORD") # Your password secret

TARGET_URLS = [
    "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10",
    "https://park4night.com/en/search?lat=37.87856774592691&lng=-8.568677272965147&z=10"
]

client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self):
        self.discovery_links = []
        self.processed_results = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        if os.path.exists(CSV_FILE):
            try:
                df = pd.read_csv(CSV_FILE)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except: pass
        return pd.DataFrame()

    async def login(self, page):
        """Modified to handle the Modal-based login flow."""
        if not P4N_USER or not P4N_PASS:
            print("ℹ️ Skipping Login: Credentials not found.")
            return

        print("🔐 Opening Login Modal...")
        try:
            # 1. Navigate to home or search to find the header
            await page.goto("https://park4night.com/en", wait_until="networkidle")
            
            # Handle cookies first if they block the header
            try: await page.click(".cc-btn-accept", timeout=3000)
            except: pass

            # 2. Click "My Account" to show dropdown, then "Login" to open Modal
            await page.click(".pageHeader-account-button")
            await page.click("button[data-bs-target='#signinModal']")

            # 3. Wait for modal to be visible and fill inputs
            await page.wait_for_selector("#signinUserId", state="visible")
            await page.fill("#signinUserId", P4N_USER)
            await page.fill("#signinPassword", P4N_PASS)
            
            # 4. Click the submit button inside the modal body or the footer
            # Note: Your snippet didn't show the submit button, 
            # but usually it's the primary button in the modal.
            await page.keyboard.press("Enter") # Safest fallback if selector is unknown
            
            await asyncio.sleep(5)
            print("✅ Login completed (check logs for specific place access next).")
        except Exception as e:
            print(f"❌ Login UI Error: {e}")

    async def analyze_batch_with_ai(self, batch_data):
        prompt = f"Analyze this list of {len(batch_data)} properties. Return a JSON ARRAY of objects:\n{json.dumps(batch_data)}"
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1,
            system_instruction="Extract p4n_id, prices in EUR, and English pros/cons for each object."
        )
        try:
            await asyncio.sleep(5) 
            response = await client.aio.models.generate_content(model=MODEL_NAME, contents=prompt, config=config)
            return json.loads(response.text)
        except Exception as e:
            print(f"🤖 AI Batch Failure: {e}")
            return []

    async def scrape_raw_data(self, page, url):
        print(f"📄 Raw Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(2, 4))
            
            # If redirected to login, the script will re-try login
            if "login" in page.url:
                await self.login(page)
                await page.goto(url, wait_until="domcontentloaded")

            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            review_els = await page.locator(".place-feedback-article-content").all()
            
            return {
                "p4n_id": p_id,
                "title": title,
                "url": url,
                "parking_cost": await self._get_dl(page, "Parking cost"),
                "service_price": await self._get_dl(page, "Price of services"),
                "reviews": [await r.inner_text() for r in review_els[:MAX_REVIEWS]]
            }
        except Exception as e:
            print(f"⚠️ Scrape Error {url}: {e}")
            return None

    async def _get_dl(self, page, label):
        try: return (await page.locator(f"dt:has-text('{label}') + dd").first.inner_text()).strip()
        except: return "N/A"

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1280, 'height': 800}
            )
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            await self.login(page)

            for url in TARGET_URLS:
                print(f"🔍 Discovery: {url}")
                try:
                    await page.goto(url, wait_until="networkidle")
                    links = await page.locator("a[href*='/place/']").all()
                    for link in links:
                        href = await link.get_attribute("href")
                        if href: self.discovery_links.append(f"https://park4night.com{href}" if href.startswith("/") else href)
                except: pass

            queue = []
            for link in list(set(self.discovery_links)):
                match = re.search(r'/place/(\d+)', link)
                if not match: continue
                p_id = match.group(1)
                if self.existing_df.empty or p_id not in self.existing_df['p4n_id'].astype(str).values:
                    queue.append(link)
                else:
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]['last_scraped'].iloc[0]
                    if (datetime.now() - last_date) > timedelta(days=7): queue.append(link)

            print(f"⚡ Processing {len(queue)} items in batches of {BATCH_SIZE}")
            for i in range(0, len(queue), BATCH_SIZE):
                batch_urls = queue[i:i + BATCH_SIZE]
                raw_batch = []
                for url in batch_urls:
                    data = await self.scrape_raw_data(page, url)
                    if data: raw_batch.append(data)

                if raw_batch:
                    ai_results = await self.analyze_batch_with_ai(raw_batch)
                    for raw_item in raw_batch:
                        ai_match = next((item for item in ai_results if str(item.get('p4n_id')) == str(raw_item['p4n_id'])), {})
                        self.processed_results.append({
                            "p4n_id": raw_item['p4n_id'],
                            "title": raw_item['title'],
                            "url": raw_item['url'],
                            "parking_min_eur": ai_match.get('parking_min', 0),
                            "parking_max_eur": ai_match.get('parking_max', 0),
                            "ai_pros": ai_match.get('pros', 'N/A'),
                            "ai_cons": ai_match.get('cons', 'N/A'),
                            "last_scraped": datetime.now()
                        })

            await browser.close()
            self._upsert_and_save()

    def _upsert_and_save(self):
        if not self.processed_results: return
        new_df = pd.DataFrame(self.processed_results)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df['last_scraped'] = pd.to_datetime(final_df['last_scraped'])
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(CSV_FILE, index=False)
        print(f"🚀 Success! Total database: {len(final_df)}")

if __name__ == "__main__":
    asyncio.run(P4NScraper().start())
