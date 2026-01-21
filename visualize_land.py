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

    # Load and clean data
    df = pd.read_csv(CSV_FILE)
    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        print("⚠️ No valid data found.")
        return

    # Initialize Map
    m = folium.Map(location=[38.5, -7.9], zoom_start=9, tiles="cartodbpositron")
    
    # FeatureGroup for markers
    marker_layer = folium.FeatureGroup(name="MainPropertyLayer")
    marker_layer.add_to(m)

    # Get the internal Leaflet ID for this layer to use in JS
    layer_variable_name = marker_layer.get_name()

    prop_types = sorted(df_clean['location_type'].unique().tolist())

    for _, row in df_clean.iterrows():
        # --- Helper: Data Cleaning & Formatting ---
        def clean_int(val):
            try:
                if pd.isna(val) or val == "": return 0
                return int(float(val))
            except: return 0

        def format_cost(val):
            if pd.isna(val) or val == "": return "N/A"
            try:
                num = float(val)
                return "Free" if num == 0 else f"{num}€"
            except: return "N/A"

        num_places = clean_int(row.get('num_places', 0))
        p_min = format_cost(row.get('parking_min_eur'))
        p_max = format_cost(row.get('parking_max_eur'))
        elec = format_cost(row.get('electricity_eur'))
        
        # Format Parking Display (Range vs Single Value)
        parking_display = f"{p_min} - {p_max}" if p_min != p_max else p_min

        # Parse Seasonality (JSON string to readable text)
        seasonality_text = "No data"
        try:
            if pd.notna(row.get('review_seasonality')):
                s_dict = json.loads(row['review_seasonality'])
                # Show last 3 months found in data
                seasonality_text = ", ".join([f"{k}: {v}" for k, v in sorted(s_dict.items())[-3:]])
        except: pass
        
        # --- Popup HTML Construction ---
        popup_html = f"""<div style="font-family: Arial; width: 320px; font-size: 13px;">
            <h3 style="margin-bottom: 5px;">{row['title']}</h3>
            <div style="color: #666; font-style: italic; margin-bottom: 10px;">{row['location_type']}</div>
            
            <b>Stats:</b> {num_places} places | <b>Rating:</b> {row['avg_rating']}⭐ ({row['total_reviews']} reviews)<br>
            <b>Costs:</b> Parking: {parking_display} | <b>Elec:</b> {elec}<br>
            <b>Languages:</b> {row.get('top_languages', 'N/A')}<br>
            <b>Recent Seasonality:</b> {seasonality_text}
            
            <div style="margin-top: 10px; border-top: 1px solid #eee; padding-top: 10px;">
                <b style="color: green;">AI Pros:</b><br>
                <span style="font-size: 11px;">{row.get('ai_pros', 'None listed')}</span>
            </div>
            <div style="margin-top: 5px;">
                <b style="color: #d35400;">AI Cons:</b><br>
                <span style="font-size: 11px;">{row.get('ai_cons', 'None listed')}</span>
            </div>

            <br><a href="{row['url']}" target="_blank" style="display: block; text-align: center; background: #2c3e50; color: white; padding: 8px; border-radius: 4px; text-decoration: none; font-weight: bold;">View on Park4Night</a>
        </div>"""

        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=350),
            icon=folium.Icon(color='green' if row['avg_rating'] >= 4 else 'orange', icon='home', prefix='fa')
        )
        
        # Store metadata for JS filtering
        marker.options['extraData'] = {
            'rating': float(row['avg_rating']),
            'places': num_places,
            'type': str(row['location_type'])
        }
        marker.add_to(marker_layer)

    # --- THE FILTER JAVASCRIPT ---
    filter_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; padding: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: fixed; z-index: 9999; }}
        #filter-panel {{ top: 20px; right: 20px; width: 220px; }}
        #debug-log {{ top: 20px; left: 60px; font-size: 10px; background: rgba(255,255,255,0.8); padding: 5px; border-radius: 4px; border: 1px solid #ccc; }}
        .btn-apply {{ background: #2c3e50; color: white; width: 100%; padding: 10px; border-radius: 6px; cursor: pointer; font-weight: bold; border: none; margin-top: 10px; }}
        .btn-reset {{ background: #95a5a6; color: white; width: 100%; padding: 10px; border-radius: 6px; cursor: pointer; font-weight: bold; border: none; margin-top: 5px; }}
    </style>

    <div id="debug-log" class="map-overlay">Status: Ready</div>

    <div id="filter-panel" class="map-overlay">
        <h3 style="margin:0;">Filters</h3>
        <p style="font-size: 12px;">Sites matching: <b id="match-count">{len(df_clean)}</b></p>
        
        <label style="font-size:11px;">Min Rating: <span id="txt-rating">0</span></label>
        <input type="range" id="range-rating" min="0" max="5" step="0.1" value="0" style="width:100%" oninput="document.getElementById('txt-rating').innerText=this.value">
        
        <label style="font-size:11px;">Min Places: <span id="txt-places">0</span></label>
        <input type="range" id="range-places" min="0" max="100" step="5" value="0" style="width:100%" oninput="document.getElementById('txt-places').innerText=this.value">
        
        <select id="sel-type" style="width:100%; margin-top:10px;">
            <option value="All">All Types</option>
            {" ".join([f'<option value="{t}">{t}</option>' for t in prop_types])}
        </select>
        
        <button onclick="applyFilters()" class="btn-apply">Apply Filters</button>
        <button onclick="resetFilters()" class="btn-reset">Reset</button>
    </div>

    <script>
    var markerStore = null;

    function log(msg) {{
        document.getElementById('debug-log').innerText = "Status: " + msg;
    }}

    function applyFilters() {{
        log("Filtering...");
        const minR = parseFloat(document.getElementById('range-rating').value);
        const minP = parseInt(document.getElementById('range-places').value);
        const type = document.getElementById('sel-type').value;

        var targetLayer = window['{layer_variable_name}'];

        if (!targetLayer) {{ 
            log("Err: Layer {layer_variable_name} not found"); 
            return; 
        }}

        if (!markerStore) {{
            markerStore = targetLayer.getLayers();
            log("Backup created: " + markerStore.length);
        }}

        targetLayer.clearLayers();

        const filtered = markerStore.filter(m => {{
            const d = m.options.extraData;
            if (!d) return false;
            return d.rating >= minR && 
                   d.places >= minP && 
                   (type === "All" || d.type === type);
        }});

        filtered.forEach(m => targetLayer.addLayer(m));
        document.getElementById('match-count').innerText = filtered.length;
        log("Match: " + filtered.length);
    }}

    function resetFilters() {{
        document.getElementById('range-rating').value = 0;
        document.getElementById('txt-rating').innerText = 0;
        document.getElementById('range-places').value = 0;
        document.getElementById('txt-places').innerText = 0;
        document.getElementById('sel-type').value = "All";
        applyFilters();
    }}
    </script>
    """
    m.get_root().html.add_child(folium.Element(filter_html))
    m.save("index.html")
    print(f"🚀 Map successfully generated for {len(df_clean)} locations.")

if __name__ == "__main__":
    generate_map()
