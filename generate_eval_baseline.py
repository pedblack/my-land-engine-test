import asyncio
import json
import os
from datetime import datetime
from google import genai
from google.genai import types
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIG ---
EVAL_MODEL = "gemini-2.5-flash" # Use higher reasoning for the golden set
TARGET_URLS = [
    "https://park4night.com/en/place/442109",
    "https://park4night.com/en/place/177331"
]
TAXONOMY_FILE = "taxonomy.json"
OUTPUT_FILE = "eval_golden_set.json"

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

def ts_print(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def load_taxonomy_for_prompt():
    with open(TAXONOMY_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
        pro_list = [f"- {item['topic']}: {item['description']}" for item in data['pros']]
        con_list = [f"- {item['topic']}: {item['description']}" for item in data['cons']]
        return "\n".join(pro_list), "\n".join(con_list)

class EvalBaselineGenerator:
    async def scrape_property(self, context, url):
        ts_print(f"🌐 Scraping: {url}")
        page = await context.new_page()
        reviews = []
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_selector(".place-feedback-article", timeout=10000)
            elements = await page.locator(".place-feedback-article-content").all()
            for el in elements[:15]: # Take top 15 reviews
                text = await el.text_content()
                if text: reviews.append(text.strip())
        except Exception as e:
            ts_print(f"⚠️ Error: {e}")
        finally:
            await page.close()
        return {"url": url, "reviews": reviews}

    async def generate_labels(self, reviews, pro_tax, con_tax):
        ts_print(f"🤖 Labeling {len(reviews)} reviews...")
        
        system_instruction = f"""You are creating a GOLDEN EVAL DATASET for a competitive analysis tool.
        Your task is to take a list of reviews and, for EACH review, identify all matching topics.

        ### TAXONOMY DEFINITIONS ###
        PRO_KEYS:
        {pro_tax}

        CON_KEYS:
        {con_tax}

        ### OUTPUT JSON SCHEMA ###
        [
            {{
                "original_review": "string",
                "labeled_topics": {{
                    "pros": ["topic_key"],
                    "cons": ["topic_key"]
                }},
                "reasoning_for_misc": "If you used a 'misc' key, explain exactly why none of the existing keys fit."
            }}
        ]"""

        response = await client.aio.models.generate_content(
            model=EVAL_MODEL,
            contents=f"REVIEWS TO LABEL:\n{json.dumps(reviews)}",
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.0, # Deterministic for eval baselines
                system_instruction=system_instruction
            )
        )
        return json.loads(response.text)

    async def run(self):
        pro_tax, con_tax = load_taxonomy_for_prompt()
        golden_dataset = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await Stealth().apply_stealth_async(context)

            for url in TARGET_URLS:
                data = await self.scrape_property(context, url)
                if data["reviews"]:
                    labels = await self.generate_labels(data["reviews"], pro_tax, con_tax)
                    golden_dataset.append({
                        "url": url,
                        "entry_timestamp": datetime.now().isoformat(),
                        "eval_results": labels
                    })

            await browser.close()

        with open(OUTPUT_FILE, "w") as f:
            json.dump(golden_dataset, f, indent=4)
        ts_print(f"✅ Baseline generated in {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(EvalBaselineGenerator().run())
