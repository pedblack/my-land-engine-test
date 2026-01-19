import os
from firecrawl import FirecrawlApp

def main():
    # 1. Initialize the App
    # Replace 'fc-YOUR_API_KEY' with your actual key or set it in your env variables
    api_key = os.getenv("FIRECRAWL_API_KEY", "fc-YOUR_API_KEY")
    app = FirecrawlApp(api_key=api_key)

    # 2. Define the target URL
    target_url = "https://www.idealista.pt/imovel/33454228/" # Example listing

    print(f"--- Starting scrape for: {target_url} ---")

    try:
        # 3. Perform the scrape (Fixed Syntax)
        scrape_result = app.scrape_url(
            target_url, 
            params={
                "formats": ["json"],
                "jsonOptions": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "price": {"type": "string"},
                            "location": {"type": "string"},
                            "description": {"type": "string"},
                            "features": {"type": "array", "items": {"type": "string"}},
                            "energy_certificate": {"type": "string"}
                        },
                        "required": ["price", "location"]
                    }
                },
                "waitFor": 3000  # Gives Idealista time to load/bypass initial check
            }
        )

        # 4. Display the results
        if scrape_result:
            print("Successfully scraped data:")
            print(scrape_result)
        else:
            print("No data returned.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
