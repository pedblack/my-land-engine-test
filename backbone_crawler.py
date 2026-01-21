import asyncio
import random
import re
import os
import json
import argparse
import time
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

# --- ADAPTED SETTINGS ---
AI_DELAY = 1.5
STALENESS_DAYS = 30
MIN_REVIEWS_THRESHOLD = 5
DEV_LIMIT = 1

# --- PARTITION SETTINGS ---
URL_LIST_FILE = "url_list.txt"   
STATE_FILE = "queue_state.json"  

# --- SYSTEM SETTINGS ---
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME") 
P4N_PASS = os.environ.get("P4N_PASSWORD") 

def ts_print(msg):
    """Prints to console with a timestamp [YYYY-MM-DD HH:MM:SS]."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")

class DailyQueueManager:
    @staticmethod
    def get_next_partition():
        if not os.path.exists(URL_LIST_FILE):
            ts_print(f"❌ ERROR: {URL_LIST_FILE} not found.")
            return [], 0, 0
        with open(URL_LIST_FILE, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        state = {"current_index": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f: state = json.load(f)
            except: pass
        idx = state.get("current_index", 0)
        if idx >= len(urls): idx = 0
        target_url = urls[idx]
        return [target_url], idx + 1, len(urls)

    @staticmethod
    def increment_state():
        if not os.path.exists(URL_LIST_FILE): return
        with open(URL_LIST_FILE, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        state = {"current_index": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f: state = json.load(f)
            except: pass
        state["current_index"] = (state.get("current_index", 0) + 1) % len(urls)
        with open(STATE_FILE, 'w') as f: json.dump(state, f)

class PipelineLogger:
    _initialized = False

    @staticmethod
    def log_event(event_type, data):
        processed_content = {}
        for k, v in data.items():
            if isinstance(v, str) and (v.strip().startswith('{') or v.strip().startswith('[')):
                try: processed_content[k] = json.loads(v)
                except: processed_content[k] = v
            else: processed_content[k] = v
        
        log_entry = {"timestamp": datetime.now().isoformat(), "type": event_type, "content": processed_content}
        
        mode = "a"
        if not PipelineLogger._initialized:
            mode = "w"
            PipelineLogger._initialized = True

        with open(LOG_FILE, mode, encoding="utf-8") as f:
            header = f"\n{'='*30} {event_type} {'='*30}\n"
            f.write(header + json.dumps(log_entry, indent=4, default=str, ensure_ascii=False) + "\n")

client = genai.Client(api_key=GEMINI_API_KEY)

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
            ts_print("⚠️ [LOGIN] Missing credentials.")
            return False

        ts_print(f"🔐 [LOGIN] Attempting for user: {P4N_USER}...")
        try:
            t_start = time.time()
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(0.5)
            await page.click(".pageHeader-account-dropdown >> text='Login'", force=True)
            await page.wait_for_selector("#signinUserId", state="visible", timeout=10000)
            
            await page.locator("#signinUserId").fill(P4N_USER)
            await page.locator("#signinPassword").fill(P4N_PASS)
            
            ts_print("⏳ [LOGIN] Submitting credentials...")
            await page.locator("#signinModal .modal-footer button[type='submit']").click()
            
            await page.wait_for_function(
                f"""() => document.querySelector('.pageHeader-account-button span')?.innerText.toLowerCase().includes('{P4N_USER.lower()}')""",
                timeout=12000
            )
            ts_print(f"✅ [LOGIN] Success (Took {time.time() - t_start:.2f}s)")
            return True
        except Exception as e: 
            ts_print(f"❌ [LOGIN] Error: {e}")
            return False

    async def analyze_with_ai(self, raw_data):
        self.stats["gemini_calls"] += 1
        t_start = time.time()
        
        system_instruction = """
Analyze the provided property data and reviews. Return JSON ONLY. Use snake_case.

Schema:
{
  "num_places": int,
  "parking_min": float,
  "parking_max": float,
  "electricity_eur": float,
  "pros_cons": {
    "pros": [ {"topic": "string", "count": int} ],
    "cons": [ {"topic": "string", "count": int} ]
  }
}

Instructions:
1. num_places: Extract from the 'places_count' field.
2. parking_min/parking_max: Extract the price range for parking. If only one price exists, set both to that value.
3. electricity_eur: Extract the cost of electricity per day/unit. If included in price, set to 0.0.
4. pros_cons: Extract common themes from reviews. List by recurrence frequency. Topics must be 3-5 words max.
5. If any numeric data is missing, return null.
"""

        json_payload = json.dumps(raw_data, default=str, ensure_ascii=False)
        config = types.GenerateContentConfig(response_mime_type="application/json", temperature=0.1, system_instruction=system_instruction)
        try:
            await asyncio.sleep(AI_DELAY) 
            response = await client.aio.models.generate_content(model=MODEL_NAME, contents=f"ANALYZE:\n{json_payload}", config=config)
            ai_json = json.loads(response.text)
            ts_print(f"🤖 [AI] Analysis complete (Took {time.time() - t_start:.2f}s)")
            PipelineLogger.log_event("GEMINI_RESPONSE", ai_json)
            return ai_json
        except Exception as e:
            PipelineLogger.log_event("GEMINI_ERROR", {"error": str(e)})
            return {}

    async def extract_atomic(self, page, url, current_num, total_num):
        ts_print(f"➡️  [{current_num}/{total_num}] Scraped Item: {url}")
        self.stats["read"] += 1
        try:
            t_nav = time.time()
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_selector(".place-feedback-average", timeout=10000)

            t_dom = time.time()
            stats_container = page.locator(".place-feedback-average")
            raw_count_text = await stats_container.locator("strong").inner_text()
            count_match = re.search(r'(\d+)', raw_count_text)
            actual_feedback_count = int(count_match.group(1)) if count_match else 0
            
            if actual_feedback_count < MIN_REVIEWS_THRESHOLD:
                ts_print(f"🗑️  [DISCARD] Low feedback ({actual_feedback_count})")
                self.stats["discarded_low_feedback"] += 1
                return

            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            
            review_articles = await page.locator(".place-feedback-article").all()
            formatted_reviews = []
            review_seasonality = {}

            for article in review_articles[:15]:
                try:
                    date_val = await article.locator("time").get_attribute("datetime") or "Unknown"
                    text_val = await article.locator(".place-feedback-article-content").inner_text()
                    formatted_reviews.append(f"[{date_val}]: {text_val.strip()}")
                    
                    # Traditional Crawler Part: Create review_seasonality field
                    if "-" in date_val:
                        month_key = date_val[:7] # Format YYYY-MM
                        review_seasonality[month_key] = review_seasonality.get(month_key, 0) + 1
                except: continue

            raw_payload = {
                "places_count": await self._get_dl(page, "Number of places"),
                "parking_cost": await self._get_dl(page, "Parking cost"),
                "all_reviews": formatted_reviews 
            }
            
            ai_data = await self.analyze_with_ai(raw_payload)
            pc = ai_data.get("pros_cons") or {}

            row = {
                "p4n_id": p_id, "title": title, "url": url, 
                "num_places": ai_data.get("num_places"),
                "parking_min": ai_data.get("parking_min"),
                "parking_max": ai_data.get("parking_max"),
                "electricity_eur": ai_data.get("electricity_eur"),
                "total_reviews": actual_feedback_count, 
                "avg_rating": float(re.search(r'(\d+\.?\d*)', await stats_container.locator(".text-gray").inner_text()).group(1)),
                "review_seasonality": json.dumps(review_seasonality),
                "ai_pros": "; ".join([f"{p['topic']} ({p['count']})" for p in pc.get('pros', [])]),
                "ai_cons": "; ".join([f"{c['topic']} ({c['count']})" for c in pc.get('cons', [])]),
                "last_scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            PipelineLogger.log_event("STORAGE_ROW", row)
            self.processed_batch.append(row)
        except Exception as e: 
            ts_print(f"  ⚠️ Error in atomic extraction: {e}")

    async def _get_dl(self, page, label):
        try: return (await page.locator(f"dt:has-text('{label}') + dd").first.inner_text()).strip()
        except: return "N/A"

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0...")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            await page.goto("https://park4night.com/en", wait_until="domcontentloaded")
            try: await page.click(".cc-btn-accept", timeout=3000)
            except: pass
            
            if not await self.login(page) and not self.is_dev:
                ts_print("🛑 [CRITICAL] Login failed.")
                await browser.close()
                return

            target_urls, current_idx, total_idx = DailyQueueManager.get_next_partition()
            ts_print(f"📅 [PARTITION] Day {current_idx} of {total_idx}")
            
            discovery_links = []
            for url in target_urls:
                ts_print(f"🔗 [SEARCH LINK] Fetching from: {url}")
                await page.goto(url, wait_until="domcontentloaded")
                try:
                    await page.wait_for_selector("a[href*='/place/']", timeout=12000)
                except:
                    ts_print("⚠️ Map results taking too long to appear.")
                
                links = await page.locator("a[href*='/place/']").all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href: discovery_links.append(f"https://park4night.com{href}" if href.startswith("/") else href)

            discovered = list(set(discovery_links))
            queue = []
            for link in discovered:
                if self.is_dev and len(queue) >= DEV_LIMIT: break
                p_id = link.split("/")[-1]
                is_stale = True
                if not self.force and not self.existing_df.empty and str(p_id) in self.existing_df['p4n_id'].astype(str).values:
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == str(p_id)]['last_scraped'].iloc[0]
                    if pd.notnull(last_date) and (datetime.now() - pd.to_datetime(last_date)) < timedelta(days=STALENESS_DAYS):
                        is_stale = False
                if is_stale or self.force: queue.append(link)
                else: self.stats["discarded_fresh"] += 1

            for i, link in enumerate(queue, 1):
                await self.extract_atomic(page, link, i, len(queue))
            
            await browser.close()
            self._upsert_and_save()
            ts_print(f"🏁 [RUN SUMMARY] Scraped: {self.stats['read']} | Gemini: {self.stats['gemini_calls']}")
            if not self.is_dev: DailyQueueManager.increment_state()

    def _upsert_and_save(self):
        if not self.processed_batch: return
        new_df = pd.DataFrame(self.processed_batch)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(self.csv_file, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev, force=args.force).start())
