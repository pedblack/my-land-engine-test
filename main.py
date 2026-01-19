import os
import csv
from firecrawl import Firecrawl

def run_land_engine():
    # 1. Initialize the latest SDK
    app = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))
    csv_file = "land_deals.csv"
    headers = ["url", "price", "area_sqm", "location", "has_water"]

    # Initialize CSV if it doesn't exist
    if not os.path.exists(csv_file):
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

    # 2. Updated Search Query Logic
    query = "site:olx.pt terreno rustico alentejo preco 50000..80000"
    print(f"🔎 Querying Firecrawl for: {query}")
    
    try:
        # CORRECT SDK CALL: Parameters are passed directly, not in a 'params' dict
        search_result = app.search(query, limit=3)
        
        # Accessing data: newer SDK returns a result object where .data is the list
        listings = search_result.get('data', []) if isinstance(search_result, dict) else getattr(search_result, 'data', [])
        
        if listings:
            # POC: Process exactly ONE listing to save credits
            target_item = listings[0]
            url = target_item.get('url')
            print(f"✨ Found link: {url}. Scraping now...")

            # CORRECT SCRAPE CALL: schema is now passed inside 'jsonOptions' or directly depending on version
            # The most stable 2026 way is using the 'formats' and 'jsonOptions' keys
            scrape_result = app.scrape(url, formats=["json"], jsonOptions={
                "schema": {
                    "type": "object",
                    "properties": {
                        "price": {"type": "integer"},
                        "area_sqm": {"type": "integer"},
                        "location": {"type": "string"},
                        "has_water": {"type": "boolean"}
                    }
                }
            })
            
            # Extract data safely
            data = scrape_result.get('json', {}) if isinstance(scrape_result, dict) else getattr(scrape_result, 'json', {})
            
            row = {
                "url": url,
                "price": data.get("price"),
                "area_sqm": data.get("area_sqm"),
                "location": data.get("location"),
                "has_water": data.get("has_water")
            }

            # 3. Append to CSV
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writerow(row)
            
            print(f"✅ Data saved: {row['price']}€ in {row['location']}")
            
        else:
            print("❌ No links found with current query.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_land_engine()
