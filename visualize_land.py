import pandas as pd
import folium
from folium.plugins import MarkerCluster, HeatMap
import json
import os

CSV_FILE = "backbone_locations.csv"
OUTPUT_MAP = "portugal_land_map.html"

def generate_map():
    if not os.path.exists(CSV_FILE):
        print("No data found to visualize.")
        return

    df = pd.read_csv(CSV_FILE)
    
    # 1. Coordinate Extraction
    # P4N URLs usually contain lat/lng: ...search?lat=37.4383&lng=-8.7558
    def get_coords(url):
        try:
            lat = re.search(r'lat=([-.\d]+)', url).group(1)
            lng = re.search(r'lng=([-.\d]+)', url).group(1)
            return float(lat), float(lng)
        except:
            return None

    # Apply coordinate extraction
    import re
    df['coords'] = df['url'].apply(get_coords)
    df = df.dropna(subset=['coords'])

    # 2. Initialize Map (Centered on Portugal)
    m = folium.Map(location=[39.3999, -8.2245], zoom_start=7, tiles="cartodbpositron")

    # 3. Add HeatMap (Density of spots)
    heat_data = [[c[0], c[1]] for c in df['coords']]
    HeatMap(heat_data, name="Spot Density", show=False).add_to(m)

    # 4. Add Individual Markers with AI Context
    marker_cluster = MarkerCluster(name="Land Details").add_to(m)

    for _, row in df.iterrows():
        # Logic for color: Green is cheap/good, Red is expensive
        color = "green" if row['parking_min_eur'] < 10 else "orange" if row['parking_min_eur'] < 25 else "red"
        
        # Build Popup HTML
        popup_html = f"""
        <div style='width:250px; font-family: sans-serif;'>
            <h4>{row['title']}</h4>
            <b>Type:</b> {row['type']}<br>
            <b>Rating:</b> ⭐{row['rating']}<br>
            <b>Price:</b> {row['parking_min_eur']}€ - {row['parking_max_eur']}€<br>
            <hr>
            <b>AI Pros:</b><br><small>{row['ai_pros']}</small><br>
            <b>AI Cons:</b><br><small>{row['ai_cons']}</small><br>
            <a href="{row['url']}" target="_blank">View on Park4Night</a>
        </div>
        """
        
        folium.Marker(
            location=row['coords'],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(color=color, icon="info-sign")
        ).add_to(marker_cluster)

    folium.LayerControl().add_to(m)
    m.save(OUTPUT_MAP)
    print(f"✅ Map generated: {OUTPUT_MAP}")

if __name__ == "__main__":
    generate_map()
