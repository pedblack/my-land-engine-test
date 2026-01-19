import asyncio
import random
import re
import os
import json
import pandas as pd
import google.generativeai as genai
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIGURATION ---
CSV_FILE = "backbone_locations.csv"
# SECURE: Pulls from GitHub Secrets or uses your provided key locally
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyD_A_bYXFkkOLzpXgPuvje39x4w7YPOfzs")
MODEL_NAME = "gemini-3-flash-preview" # Latest Jan 2026 SOTA Model

TARGET_URLS = [
    "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10",
    "https://park4night.com/en/search?lat=37.87856774592691&lng=-8.568677272965147&z=10"
]

# Initialize AI
genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel(
    model_name=MODEL_NAME,
    generation_config={"response_mime_type": "application/json", "temperature": 0.1}
)

class P4NScraper:
    def __init__(self):
        self.discovery_links = []
        self.scraped_batch = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        """Loads state from CSV to enable incremental logic."""
        if os.path.exists(CSV_FILE):
            try:
                df = pd.read_csv(CSV_FILE)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except Exception as e:
                print(f"⚠️ Error loading state: {e}")
        return pd.DataFrame(columns=["p4n_id", "last_scraped"])

    async def init_browser(self, p):
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800}
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        return browser, page

    async def run_discovery(self, page):
        """Phase 1: Map Discovery."""
        for url in TARGET_URLS:
            print(f"🔍 Discovering: {url}")
            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)
                try: # Clear Cookie Banner
                    btn = page.locator(".cc-btn-accept")
                    if await btn.is_visible(timeout=3000): await btn.click()
                except: pass
                
                await asyncio.sleep(3)
                links = await page.locator("#searchmap-list-results li a").all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href and "/place/" in href:
                        self.discovery_links.append(f"https://park4night.com{href}")
            except Exception as e:
                print(f"❌ Discovery failed for {url}: {e}")

    async def summarize_via_ai(self, reviews):
        """Phase 2b: Intelligent Signal Extraction using Gemini 3."""
        if not reviews:
            return "No reviews.", "N/A", "{}"
        
        prompt = f"""
        Analyze these campsite reviews. Provide:
        - 'pros': 2-3 specific positives (English).
        - 'cons': 2-3 recurrent issues (English).
        - 'lang_dist': JSON dict of ISO codes and their review counts.
        
        Reviews: {" ".join(reviews)[:10000]}
        """
        try:
            await asyncio.sleep(4) # Rate limit safety
            response = await ai_model.generate_content_async(prompt)
            data = json.loads(response.text)
            return (
                data.get('pros', 'N/A'), 
                data.get('cons', 'N/A'), 
                json.dumps(data.get('lang_dist', {}))
            )
        except Exception as e:
            print(f"🤖 AI Error: {e}")
            return "Analysis failed.", "N/A", "{}"

    async def extract_atomic(self, page, url):
        """Phase 2: Atomic Extraction (Detail + Comments + AI)."""
        print(f"📄 Atomic Scrape: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(2, 4))

            # Metadata Scraping
            p4n_id = await page.locator("body").get_attribute("data-place-id")
            title = (await page.locator(".place-header-name").inner_text()).strip()
            loc_type = await page.locator(".place-header-access img").get_attribute("title")
            
            # Rating & Feedback counts
            rating_el = page.locator(".rating-note").first
            rating = (await rating_el.inner_text()).split('/')[0] if await rating_el.count() > 0 else "0"
            
            # Review Scraping
            review_els = await page.locator(".place-feedback-article-content").all()
            raw_reviews = [await r.inner_text() for r in review_els]
            
            # AI Enrichment
            pros, cons, lang_dist = await self.summarize_via_ai(raw_reviews)

            # DL Table Parsing
            async def get_dl(label):
                try: return (await page.locator(f"dl.place-info-details dt:has-text('{label}') + dd").inner_text()).strip()
                except: return "N/A"

            self.scraped_batch.append({
                "p4n_id": p4n_id,
                "title": title,
                "type": loc_type,
                "
