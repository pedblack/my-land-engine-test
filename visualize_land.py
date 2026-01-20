import pandas as pd
import folium
import os
import json
import numpy as np

CSV_FILE = os.environ.get("CSV_FILE", "backbone_locations.csv")

def generate_map():
    if not os.path.exists(CSV_FILE):
        print(f"❌ {CSV_FILE} not found.")
        return

    df = pd.read_csv(CSV_FILE)
    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        print("⚠️ No valid data found.")
        return

    m = folium.Map(location=[38.5, -7.9], zoom_start=9, tiles="cartodbpositron")
    
    # We use a unique ID for the FeatureGroup to ensure JS finds it
    marker_layer = folium.FeatureGroup(name="MainPropertyLayer").add_to(m)

    prop_types = sorted(df_clean['location_type'].unique().tolist())

    for _, row in df_clean.iterrows():
        def clean_int(val):
            try:
                if pd.isna(val) or val == "": return 0
                return int(float(val))
            except: return 0

        num_places = clean_int(row.get('num_places', 0))
        intensity = float(row.get('intensity_index', 0)) if not pd.isna(row.get('intensity_index')) else 0
        
        popup_html = f"""<div style="font-family: Arial; width: 300px;">
            <h3>{row['title']}</h3>
            <b>Rating:</b> {row['avg_rating']} ⭐ | <b>Places:</b> {num_places}
            <br><a href="{row['url']}" target="_blank">View on Park4Night</a>
        </div>"""

        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=350),
            icon=folium.Icon(color='green' if row['avg_rating'] >= 4 else 'orange', icon='home', prefix='fa')
        )
        
        # KEY: We bind data to a specific 'extra_data' object inside options to avoid conflicts
        marker.options['extra_data'] = {
            'rating': float(row['avg_rating']),
            'places': num_places,
            'type': str(row['location_type'])
        }
        marker.add_to(marker_layer)

    filter_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; padding: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: fixed; z-index: 9999; }}
        #filter-panel {{ top: 20px; right: 20px; width: 220px; }}
    </style>

    <div id="filter-panel" class="map-overlay">
        <h3 style="margin:0 0 10px 0;">Filters</h3>
        <p>Visible: <b id="match-count">{len(df_clean)}</b></p>
        
        <label>Min Rating: <span id="txt-rating">0</span></label>
        <input type="range" id="range-rating" min="0" max="5" step="0.1" value="0" style="width:100%" oninput="document.getElementById('txt-rating').innerText=this.value">
        
        <label>Min Places: <span id="txt-places">0</span></label>
        <input type="range" id="range-places" min="0" max="100" step="5" value="0" style="width:100%" oninput="document.getElementById('txt-places').innerText=this.value">
        
        <label>Type:</label>
        <select id="sel-type" style="width:100%; margin-top:5px;">
            <option value="All">All Types</option>
            {" ".join([f'<option value="{t}">{t}</option>' for t in prop_types])}
        </select>
        
        <button onclick="applyFilters()" style="width:100%; margin-top:15px; background:#2c3e50; color:white; padding:10px; border-radius:5px; cursor:pointer;">Apply</button>
    </div>

    <script>
    var markerStore = null;

    function applyFilters() {{
        console.log("Starting Filter Process...");
        const minR = parseFloat(document.getElementById('range-rating').value);
        const minP = parseInt(document.getElementById('range-places').value);
        const type = document.getElementById('sel-type').value;

        let layerGroup = null;
        
        // Loop through all window objects to find our FeatureGroup
        for (let key in window) {{
            if (window[key] && window[key] instanceof L.LayerGroup) {{
                // Identify by looking for our markers
                let layers = window[key].getLayers();
                if (layers.length > 0 && layers[0].options.extra_data) {{
                    layerGroup = window[key];
                    console.log("Found target layer with " + layers.length + " markers.");
                    break;
                }}
            }}
        }}

        if (!layerGroup) {{
            console.error("Could not find the marker layer group!");
            return;
        }}

        // Save all markers on the first run
        if (!markerStore) {{
            markerStore = layerGroup.getLayers();
            console.log("Initial backup created: " + markerStore.length + " markers.");
        }}

        layerGroup.clearLayers();

        const filtered = markerStore.filter(m => {{
            const data = m.options.extra_data;
            if (!data) return false;
            
            const matchR = data.rating >= minR;
            const matchP = data.places >= minP;
            const matchT = (type === "All" || data.type === type);
            
            return matchR && matchP && matchT;
        }});

        console.log("Matching results: " + filtered.length);
        filtered.forEach(m => layerGroup.addLayer(m));
        document.getElementById('match-count').innerText = filtered.length;
    }}
    </script>
    """
    m.get_root().html.add_child(folium.Element(filter_html))
    m.save("index.html")
    print("🚀 Map generated. Use 'F12' in browser to see debug logs if filtering fails.")

if __name__ == "__main__":
    generate_map()
