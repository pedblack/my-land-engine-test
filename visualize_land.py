import pandas as pd
import folium
from folium.plugins import MarkerCluster
import os

# Select source based on environment
CSV_FILE = os.environ.get("CSV_FILE", "backbone_locations.csv")

def generate_map():
    if not os.path.exists(CSV_FILE):
        print(f"❌ {CSV_FILE} not found. Skip map generation.")
        return

    df = pd.read_csv(CSV_FILE)
    
    # Filter rows that have valid coordinates
    df = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df.empty:
        print("⚠️ No coordinates found in CSV. Map cannot be generated.")
        return

    # Center on Portugal
    m = folium.Map(location=[39.5, -8.0], zoom_start=7, tiles="cartodbpositron")
    
    # Use MarkerCluster for cleaner look if locations are dense
    marker_cluster = MarkerCluster().add_to(m)

    for _, row in df.iterrows():
        # Build enriched tooltip with new pricing details
        # Max price and electricity cost now included
        popup_html = f"""
        <div style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; width: 220px; line-height: 1.5;">
            <h4 style="margin-top: 0; color: #2c3e50; border-bottom: 1px solid #eee; padding-bottom: 5px;">{row['title']}</h4>
            <div style="font-size: 0.9em; margin-bottom: 8px;">
                <span title="Minimum Daily Price">💰 <b>Min:</b> {row['parking_min_eur']}€</span><br>
                <span title="Maximum Daily Price">💸 <b>Max:</b> {row['parking_max_eur']}€</span><br>
                <span title="Electricity Surcharge">⚡ <b>Elec:</b> {row['electricity_eur'] if row['electricity_eur'] > 0 else 'N/A'}€</span>
            </div>
            <div style="font-size: 0.85em; background: #f9f9f9; padding: 5px; border-radius: 4px; border-left: 3px solid #27d9a1;">
                <b>Pros:</b> {row['ai_pros']}<br>
                <b>Cons:</b> {row['ai_cons']}
            </div>
            <div style="margin-top: 10px; font-size: 0.8em; color: #7f8c8d;">
                ⭐ {row['avg_rating']}/5 ({row['total_reviews']} reviews)
            </div>
            <hr style="border: 0; border-top: 1px solid #eee; margin: 10px 0;">
            <a href="{row['url']}" target="_blank" style="display: block; text-align: center; background: #27d9a1; color: white; text-decoration: none; padding: 5px; border-radius: 3px; font-weight: bold;">Open in Park4Night</a>
        </div>
        """
        
        # Color code based on price: Green < 15€, Orange 15-25€, Red > 25€
        icon_color = 'green'
        if row['parking_min_eur'] > 25:
            icon_color = 'red'
        elif row['parking_min_eur'] > 15:
            icon_color = 'orange'

        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(color=icon_color, icon='info-sign'),
            tooltip=row['title']
        ).add_to(marker_cluster)

    m.save("portugal_land_map.html")
    print("🚀 Map successfully generated with enriched pricing: portugal_land_map.html")

if __name__ == "__main__":
    generate_map()
