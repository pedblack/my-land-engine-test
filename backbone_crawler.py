import asyncio
import random
import re
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
# New SDK
from google import genai
from google.genai import types

# --- SETTINGS ---
CSV_FILE = "backbone_locations.csv"
MODEL_ID = "gemini-2.0-flash" 

class P4NScraper:
    def __init__(self):
        # Explicitly fetch the key inside the constructor
        self.api_key = os.environ.get("GOOGLE_API_KEY")
        if not self.api_key:
            # Fallback for local testing - replace with your key IF testing locally
            self.api_key = "AIzaSyD_A_bYXFkkOLzpXgPuvje39x4w7YPOfzs"
            
        if not self.api_key:
            raise ValueError("CRITICAL: GOOGLE_API_KEY not found in environment.")

        # Initialize the client here so it's fresh for every instance
        self.client = genai.Client(api_key=self.api_key)
        
        self.discovery_links = []
        self.processed_batch = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        if os.path.exists(CSV_FILE):
            try:
                df = pd.read_csv(CSV_FILE)
                if 'last_scraped' in df.columns:
                    df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except: pass
        return pd.DataFrame()

    # ... (init_browser and run_discovery stay the same) ...

    async def analyze_with_ai(self, raw_data):
        """Now uses the client initialized in __init__."""
        prompt = f"""
        Analyze property raw data. 
        1. Normalize 'parking_cost' and 'service_price' to EUR numeric.
        2. Provide 'parking_min', 'parking_max', and 'service_price_clean'.
        3. Synthesize 'pros' and 'cons' in English.
        4. Provide 'lang_dist' dict (ISO: count).
        RAW DATA: {json.dumps(raw_data)}
        """
        try:
            # Using the self.client instance
            response = self.client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.1
                )
            )
            return json.loads(response.text)
        except Exception as e:
            print(f"🤖 AI Failure: {e}")
            return {}

    # ... (Rest of the script remains the same, just ensure you call self.analyze_with_ai) ...

# Ensure the main execution uses the class properly
async def main():
    scraper = P4NScraper()
    await scraper.start()

if __name__ == "__main__":
    asyncio.run(main())
