import asyncio
import random
import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuration
BASE_URL = "https://www.park4night.com"
SEARCH_URL = f"{BASE_URL}/en/search?lat=39.3999&lng=-8.2245&z=7" # Centered on Portugal
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
]

class P4NScraper:
    def __init__(self):
        self.results = []

    async def init_browser(self, p):
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 375, 'height': 812}, # Mobile view
            is_mobile=True
        )
        page = await context.new_page()
        await stealth_async(page)
        return browser, page

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def scrape_portugal(self):
        async with async_playwright() as p:
            browser, page = await self.init_browser(p)
            try:
                print(f"Navigating to {SEARCH_URL}...")
                await page.goto(SEARCH_URL, wait_until="networkidle")
                
                # Human-like delay
                await asyncio.sleep(random.uniform(2, 5))

                # Handle Cookie Consent if it appears
                try:
                    await page.click("button:has-text('Accept')", timeout=5000)
                except:
                    pass

                # Selector logic for Park4Night (Simplified for demonstration)
                # In a real scenario, we would iterate through the list view
                locators = await page.locator(".card-location").all()
                
                for i, loc in enumerate(locators[:10]): # Sample limit
                    p4n_id = await loc.get_attribute("data-id")
                    title = await loc.locator(".title").inner_text()
                    category = await loc.locator(".category").inner_text()
                    coords_raw = await loc.get_attribute("data-coords") # e.g., "38.7,-9.1"
                    rating = await loc.locator(".rating-value").inner_text()

                    lat, lng = map(float, coords_raw.split(','))

                    self.results.append({
                        "p4n_id": p4n_id,
                        "title": title.strip(),
                        "category": category.strip(),
                        "latitude": lat,
                        "longitude": lng,
                        "rating": float(rating) if rating else 0.0
                    })
                
                return self.results

            finally:
                await browser.close()

async def main():
    scraper = P4NScraper()
    data = await scraper.scrape_portugal()
    df = pd.DataFrame(data)
    df.to_csv("backbone_locations.csv", index=False)
    print("Scraping complete. Saved to backbone_locations.csv")

if __name__ == "__main__":
    asyncio.run(main())
