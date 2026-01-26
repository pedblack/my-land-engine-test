import asyncio
import json
import os
import re
import pandas as pd
from datetime import datetime
from google import genai
from google.genai import types
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIG ---
DISCOVERY_MODEL = "gemini-2.5-flash" # Use Flash for better reasoning during discovery
URL_LIST_FILE = "url_list.txt"
OUTPUT_FILE = "taxonomy_discovery_report.json"
BATCH_SIZE = 1

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

def ts_print(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

class TaxonomyDiscoverer:
    def __init__(self):
        self.all_outliers = []
        self.suggested_keys = []

    async def scrape_url(self, context, url):
        ts_print(f"🌐 Scraping: {url}")
        page = await context.new_page()
        reviews = []
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_selector(".place-feedback-article", timeout=10000)
            
            elements = await page.locator(".place-feedback-article-content").all()
            for el in elements[:20]: # Grab top 20 reviews per property
                text = await el.text_content()
                if text: reviews.append(text.strip())
        except Exception as e:
            ts_print(f"⚠️ Failed {url}: {e}")
        finally:
            await page.close()
        return {"url": url, "reviews": reviews}

    async def analyze_batch(self, batch_data):
        ts_print(f"🤖 Analyzing batch of {len(batch_data)} properties...")
        
        system_instruction = """You are a qualitative data analyst. I am providing reviews for camping locations. 
        Your goal is to find themes that DO NOT fit into my current taxonomy.

        ### CURRENT TAXONOMY ###
        PROS: atmosphere, scenery, staff, facilities, showers, laundry, pitches, value, location, supplies, utilities, safety, pets, family.
        CONS: noise, cleanliness, broken_facilities, terrain, lack_of_shade, access_issues, price, wifi, pests, rules.

        ### TASK ###
        1. Identify specific feedback points that are too unique or specific for the keys above.
        2. For each "outlier", suggest a new 'snake_case' key.
        3. Extract the exact quote from the review.

        ### OUTPUT JSON SCHEMA ###
        {
            "new_suggestions": [
                {"suggested_key": "string", "reasoning": "string", "example_quote": "string"}
            ]
        }"""

        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.2,
            system_instruction=system_instruction,
        )

        response = await client.aio.models.generate_content(
            model=DISCOVERY_MODEL,
            contents=f"DATA TO ANALYZE:\n{json.dumps(batch_data)}",
            config=config,
        )
        
        try:
            return json.loads(response.text)
        except:
            ts_print("❌ Failed to parse AI JSON")
            return {"new_suggestions": []}

    async def run(self):
        if not os.path.exists(URL_LIST_FILE):
            ts_print("❌ No url_list.txt found.")
            return

        with open(URL_LIST_FILE, "r") as f:
            urls = [line.strip() for line in f if line.strip()][:15] # Discovery sample

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await Stealth().apply_stealth_async(context)

            # Process in chunks of 5
            for i in range(0, len(urls), BATCH_SIZE):
                batch_urls = urls[i:i + BATCH_SIZE]
                
                # Scrape concurrent batch
                scrape_tasks = [self.scrape_url(context, u) for u in batch_urls]
                batch_results = await asyncio.gather(*scrape_tasks)
                
                # AI Analysis of batch
                analysis = await self.analyze_batch(batch_results)
                self.suggested_keys.extend(analysis.get("new_suggestions", []))

            await browser.close()

        # Save results
        with open(OUTPUT_FILE, "w") as f:
            json.dump({
                "discovery_timestamp": datetime.now().isoformat(),
                "total_suggestions": len(self.suggested_keys),
                "suggestions": self.suggested_keys
            }, f, indent=4)
        
        ts_print(f"✅ Discovery complete. Report saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(TaxonomyDiscoverer().run())
