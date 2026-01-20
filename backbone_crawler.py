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
        """Saves deeply formatted JSON events to the log file for maximum readability."""
        processed_content = {}
        for k, v in data.items():
            if isinstance(v, str) and (v.strip().startswith('{') or v.strip().startswith('[')):
                try:
                    processed_content[k] = json.loads(v)
                except:
                    processed_content[k] = v
            else:
                processed_content[k] = v

        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "content": processed_content
        }
        
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            header = f"\n{'='*30} {event_type} {'='*30}\n"
            pretty_json = json.dumps(log_entry, indent=4, default=str, ensure_ascii=False)
            f.write(header + pretty_json + "\n")

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
        self.current_max_reviews = 5 if is_dev else MAX_REVIEWS 
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
        """Reliable login with simulated human typing speed."""
        if not P4N_USER or not P4N_PASS: return
        print(f"🔐 Attempting Login for {P4N_USER}...")
        try:
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(2)
            await page.click(".pageHeader-account-dropdown >> text='Login'", force=True)
            
            user_input = page.locator("#signinUserId")
            await user_input.wait_for(state="visible")
            await user_input.focus()
            
            await user_input.type(P4N_USER, delay=random.randint(150, 250))
            await asyncio.sleep(random.uniform(0.5, 1.0))
            
            pass_input = page.locator("#signinPassword")
            await pass_input.focus()
            await pass_input.type(P4N_PASS, delay=random.randint(150, 250))
            
            submit_btn = page.locator(".modal-footer button[type='submit']:has-text('Login')")
            await submit_btn.click(force=True)
            
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(5) 

            user_span = page.locator(".pageHeader-account-button span")
            actual_username = await user_span.inner_text()
            
            if P4N_USER.lower() in actual_username.lower():
                print(f"✅ Logged in as: {actual_username}")
                PipelineLogger.log_event("LOGIN_SUCCESS", {"user": actual_username})
            else:
                print(f"⚠️ Verification Failed. Found: '{actual_username}'")
                await PipelineLogger.save_screenshot(page, "login_verify_failed")
        except Exception as e:
            print(f"❌ Login UI Error: {e}")
            PipelineLogger.log_event("LOGIN_ERROR", {"error": str(e)})

    async def analyze_with_ai(self, raw_data):
        """AI analysis with strict JSON schema and detailed prompt logging."""
        system_instruction = (
            "You are a property data analyst. Analyze the provided reviews and return a JSON object with this schema: "
            "{ 'parking_min': integer_price_in_eur, 'pros': 'summary_string', 'cons': 'summary_string' }. "
            "If no price is mentioned, set parking_min to 0. Summarize reviews into concise English pros and cons."
        )
        
        json_payload = json.dumps(raw_data, default=str, ensure_ascii=False)
        
        PipelineLogger.log_event("AI_REQUEST", {
            "p4n_id": raw_data.get("p4n_id"),
            "system_instruction": system_instruction,
            "final_full_prompt": f"DATA TO ANALYZE:\n{json_payload}"
        })

        config = types.GenerateContentConfig(
            response_mime_type="application/json", 
            temperature=0.1,
            system_instruction=system_instruction
        )
        
        try:
            await asyncio.sleep(AI_DELAY) 
            response = await client.aio.models.generate_content(
                model=MODEL_NAME, 
                contents=f"DATA TO ANALYZE:\n{json_payload}", 
                config=config
            )
            PipelineLogger.log_event("AI_RESPONSE", {"res": response.text})
            return json.loads(response.text)
        except Exception as e:
            PipelineLogger.log_event("AI_ERROR", {"err": str(e)})
            return {}

    async def extract_atomic(self, page, url):
        print(f"📄 Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded")
            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            
            # --- COORDINATE EXTRACTION ---
            lat, lng = 0.0, 0.0
            coord_link = await page.locator("a[href*='lat='][href*='lng=']").first.get_attribute("href")
            if coord_link:
                lat_match = re.search(r'lat=([-+]?\d*\.\d+|\d+)', coord_link)
                lng_match = re.search(r'lng=([-+]?\d*\.\d+|\d+)', coord_link)
                if lat_match and lng_match:
                    lat, lng = float(lat_match.group(1)), float(lng_match.group(1))

            # --- RATING & REVIEW COUNT EXTRACTION ---
            total_reviews = 0
            avg_rating = 0.0
            try:
                stats_container = page.locator(".place-feedback-average")
                raw_count_text = await stats_container.locator("strong").inner_text()
                raw_rating_text = await stats_container.locator(".text-gray").inner_text()
                
                # Extract 193 from "Average (193 Feedback)"
                count_match = re.search(r'(\d+)', raw_count_text)
                if count_match: total_reviews = int(count_match.group(1))
                
                # Extract 3.82 from "3.82/5"
                rating_match = re.search(r'(\d+\.?\d*)', raw_rating_text)
                if rating_match: avg_rating = float(rating_match.group(1))
            except Exception as e:
                print(f"⚠️ Rating extraction failed: {e}")

            # --- REVIEW EXPANSION LOOP ---
            for _ in range(10): 
                reviews = await page.locator(".place-feedback-article-content").all()
                if len(reviews) >= self.current_max_reviews: break
                
                more_btn = page.locator(".place-feedback-pagination button:has-text('More')")
                if await more_btn.is_visible():
                    await more_btn.click()
                    await asyncio.sleep(random.uniform(1.5, 2.5))
                else: break

            review_els = await page.locator(".place-feedback-article-content").all()
            raw_payload = {
                "p4n_id": p_id,
                "reviews": [await r.text_content() for r in review_els[:self.current_max_reviews]]
            }
            ai_data = await self.analyze_with_ai(raw_payload)

            row = {
                "p4n_id": p_id, "title": title, "url": url,
                "latitude": lat, "longitude": lng,
                "total_reviews": total_reviews,
                "avg_rating": avg_rating,
                "parking_min_eur": ai_data.get("parking_min", 0),
                "ai_pros": ai_data.get("pros", "N/A"),
                "ai_cons": ai_data.get("cons", "N/A"),
                "last_scraped": datetime.now()
            }
            
            PipelineLogger.log_event("STORAGE_ROW", row)
            self.processed_batch.append(row)
        except Exception as e: 
            print(f"⚠️ Error {url}: {e}")

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            await page.goto("https://park4night.com/en", wait_until="networkidle")
            try: await page.click(".cc-btn-accept", timeout=3000)
            except: pass
            await self.login(page)

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

            queue = []
            for link in list(set(self.discovery_links)):
                match = re.search(r'/place/(\d+)', link)
                if not match: continue
                p_id = match.group(1)
                if self.is_dev:
                    queue.append(link)
                    break
                is_stale = True
                if not self.existing_df.empty and p_id in self.existing_df['p4n_id'].astype(str).values:
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]['last_scraped'].iloc[0]
                    if (datetime.now() - last_date) < timedelta(days=7): is_stale = False
                if is_stale: queue.append(link)

            print(f"⚡ Processing {len(queue)} items...")
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
    parser.add_argument('--dev', action='store_true')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev).start())
