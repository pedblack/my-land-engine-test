import os
import csv
from firecrawl import Firecrawl

def run_land_engine():
    # 1. Initialize the 2026 SDK
    app = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))
    csv_file = "land_deals.csv"
    headers = ["url", "price", "area_sqm", "location", "has_water"]

    # Ensure CSV exists
    if not os.path.exists(csv_file):
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

    # 2. Direct Discovery: Go to the Alentejo Land category
    list_url = "https://www.olx.pt/imoveis/terrenos-quintas/alentejo/"
    print(f"🔎 DEBUG: Accessing live listings at: {list_url}")
    
    try:
        # Scrape the page to find all links
        list_page = app.scrape(list_url, formats=["links"])
        
        # Extract links (handling both dict and object returns)
        all_links = list_page.get('links', []) if isinstance(list_page, dict) else getattr(list_page, 'links', [])
        
        # Filter for actual listing links
        listing_links = [l for l in all_links if "/d/anuncio/" in l]
        print(f"🔎 DEBUG: Found {len(listing_links)} live property links.")

        if not listing_links:
            print("❌ No links found. Check if the URL is correct or if OLX is blocking.")
            return

        # --- MAX 1 POC ---
        target_url = listing_links[0]
        print(f"✨ Target found: {target_url}. Scraping details...")

        # 3. AI Extraction
        scrape_result = app.scrape(target_url, formats
