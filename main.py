import os
import csv
import json
from firecrawl import Firecrawl

def run_land_engine():
    # 1. Initialize Firecrawl (Using the 2026 SDK naming)
    app = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))
    csv_file = "land_deals.csv"
    headers = ["url", "price", "area_sqm", "location", "has_water"]

    # Ensure CSV exists
    if not os.path.exists(csv_file):
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

    # 2. Broad Search Strategy
    # Using a simpler query to ensure Firecrawl finds results
    query = "terrenos rusticos baratos Portugal OLX"
    print(f"🔎 DEBUG: Searching Firecrawl for query: '{query}'")
    
    try:
        # We pass parameters directly to the search method
        search_result = app.search(query, limit=5)
        
        # Logging the raw structure to help you debug
        print(f"🔎 DEBUG: Raw Search Result Type: {type(search_result)}")
        
        # Safe extraction of links
        if isinstance(search_result, dict):
            listings = search_result.get('data', [])
        else:
            listings = getattr(search_result, 'data', [])

        print(f"🔎 DEBUG: Number of links found: {len(listings)}")

        if not listings:
            print("❌ No links found. The search index might be empty for this query.")
            return

        # 3. Filter for a valid OLX listing
        target_url = None
        for item in listings:
            link = item.get('url', '')
            print(f"🔗 DEBUG: Checking link: {link}")
            if "olx.pt" in link and "/d/anuncio/" in link:
                target_url = link
                break
        
        # If no OLX link specifically, just take the first result to prove it works
        if not target_url:
            target_url = listings[0].get('url')
            print(f"⚠️ Warning: No direct OLX link found. Falling back to: {target_url}")

        # 4. Scrape the single target (Cost: 1 Credit)
        print(f"✨ Scraping now: {target_url}")
        scrape_result = app.scrape(target_url, formats=["json"], jsonOptions={
            "schema": {
                "type": "object",
                "properties": {
                    "price": {"type": "integer"},
                    "area_sqm": {"type": "integer"},
                    "location": {"type": "string"},
                    "has_water": {"type": "boolean"}
                },
                "required": ["price"]
            }
        })

        # Process data
        if isinstance(scrape_result, dict):
            data = scrape_result.get('json', {})
        else:
            data = getattr(scrape_result, 'json', {})

        if not data:
            print("⚠️ Scrape succeeded but AI returned no data. Check page content.")
            return

        row = {
            "url": target_url,
            "price": data.get("price"),
            "area_sqm": data.get("area_sqm"),
            "location": data.get("location"),
            "has_water": data.get("has_water")
        }

        # 5. Final Save
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerow(row)
        
        print(f"✅ SUCCESS! Saved: {row['price']}€ listing in {row['location']}")

    except Exception as e:
        print(f"❌ FATAL ERROR: {str(e)}")
        # Print more context if available
        if hasattr(e, 'response'):
            print(f"Response Status: {e.response.status_code}")
            print(f"Response Body: {e.response.text}")

if __name__ == "__main__":
    run_land_engine()
