import pandas as pd
import folium
from folium.plugins import MarkerCluster
import os
import json

CSV_FILE = os.environ.get("CSV_FILE", "backbone_locations.csv")

def generate_map():
    if not os.path.exists(CSV_FILE):
        print(f"❌ {CSV_FILE} not found.")
        return

    df = pd.read_csv(CSV_FILE)
    # Remove entries without valid GPS data
    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        print("⚠️ No valid coordinates found.")
        return

    # --- MAP SETTINGS: CENTERED ON ALENTEJO ---
    # Alentejo center: approx 38.4, -8.0 | Zoom 8 provides a regional view
    m = folium.Map(location=[38.4, -8.0], zoom_start=8, tiles="cartodbpositron")
    
    # We use a custom FeatureGroup so our JS can easily access markers
    marker_group = folium.FeatureGroup(name="Properties")
    m.add_child(marker_group)

    # Unique property types for the filter dropdown
    prop_types = sorted(df_clean['location_type'].unique().tolist())

    for _, row in df_clean.iterrows():
        # Clean semicolon lists into HTML bullets
        def format_list(text):
            if pd.isna(text) or text == "N/A" or not str(text).strip(): return "<li>None</li>"
            return "".join([f"<li>{item.strip()}</li>" for item in str(text).split(";")])

        popup_html = f"""
        <div style="font-family: sans-serif; width: 240px; line-height: 1.4;">
            <h4 style="margin: 0 0 5px 0; border-bottom: 2px solid #27d9a1;">{row['title']}</h4>
            <small><b>{row['location_type']}</b> | {row['num_places']} places</small><br>
            <div style="background: #f8f9fa; padding: 5px; margin: 5px 0; border-radius: 4px; font-size: 0.9em;">
                💰 {row['parking_min_eur']}€ - {row['parking_max_eur']}€ | ⚡ {row['electricity_eur']}€
            </div>
            <div style="font-size: 0.85em;">
                <b>Pros:</b> <ul style="margin:2px; padding-left:15px;">{format_list(row['ai_pros'])}</ul>
                <b>Cons:</b> <ul style="margin:2px; padding-left:15px;">{format_list(row['ai_cons'])}</ul>
            </div>
            <div style="border-top: 1px solid #eee; padding-top: 5px; font-size: 0.8em;">
                ⭐ {row['avg_rating']} ({row['total_reviews']} reviews)
            </div>
            <a href="{row['url']}" target="_blank" style="display:block; text-align:center; background:#27d9a1; color:white; padding:5px; border-radius:3px; text-decoration:none; margin-top:8px;">View on P4N</a>
        </div>
        """

        # Set marker color based on rating: Green >= 4, Orange 3-4, Red < 3
        icon_color = 'green' if row['avg_rating'] >= 4 else 'orange' if row['avg_rating'] >= 3 else 'red'

        # Create marker with custom 'options' for JS filtering
        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(color=icon_color, icon='home', prefix='fa'),
            tooltip=row['title']
        )
        
        # Inject custom data for the JS filters to read
        marker.add_to(marker_group)
        marker.options['data_rating'] = float(row['avg_rating'])
        marker.options['data_reviews'] = int(row['total_reviews'])
        marker.options['data_type'] = str(row['location_type'])

    # --- CLIENT-SIDE FILTER PANEL (HTML/JS) ---
    filter_html = f"""
    <div id="filter-panel" style="position: fixed; top: 10px; right: 10px; z-index: 9999; 
         background: white; padding: 15px; border-radius: 8px; box-shadow: 0 0 15px rgba(0,0,0,0.2);
         font-family: sans-serif; width: 200px;">
        <h3 style="margin-top:0; font-size:16px;">Filters</h3>
        
        <label>Min Rating: <span id="val-rating">0</span></label><br>
        <input type="range" id="filter-rating" min="0" max="5" step="0.5" value="0" style="width:100%"><br><br>
        
        <label>Min Reviews: <span id="val-reviews">0</span></label><br>
        <input type="range" id="filter-reviews" min="0" max="500" step="10" value="0" style="width:100%"><br><br>
        
        <label>Property Type:</label><br>
        <select id="filter-type" style="width:100%">
            <option value="All">All Types</option>
            {" ".join([f'<option value="{t}">{t}</option>' for t in prop_types])}
        </select>
        
        <button onclick="applyFilters()" style="width:100%; margin-top:15px; background:#27d9a1; border:none; 
                color:white; padding:8px; border-radius:4px; cursor:pointer; font-weight:bold;">Apply</button>
    </div>

    <script>
    function applyFilters() {{
        var minRate = parseFloat(document.getElementById('filter-rating').value);
        var minRev = parseInt(document.getElementById('filter-reviews').value);
        var type = document.getElementById('filter-type').value;
        
        document.getElementById('val-rating').innerText = minRate;
        document.getElementById('val-reviews').innerText = minRev;

        // Access the marker group from the Folium map
        // Note: 'marker_group' in JS corresponds to the FeatureGroup created in Python
        var map_obj = Object.values(window).find(v => v instanceof L.Map);
        var layers = [];
        map_obj.eachLayer(function(layer) {{
            if (layer instanceof L.Marker && layer.options.data_rating !== undefined) {{
                var show = true;
                if (layer.options.data_rating < minRate) show = false;
                if (layer.options.data_reviews < minRev) show = false;
                if (type !== "All" && layer.options.data_type !== type) show = false;
                
                if (show) {{
                    layer.addTo(map_obj);
                }} else {{
                    map_obj.removeLayer(layer);
                }}
            }}
        }});
    }}
    </script>
    """
    m.get_root().html.add_child(folium.Element(filter_html))

    m.save("portugal_land_map.html")
    print("🚀 Map successfully generated with Alentejo center and Range Filters.")

if __name__ == "__main__":
    generate_map()
