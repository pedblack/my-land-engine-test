import pandas as pd
import folium
import os
import json
import numpy as np

# Use environment variables to support both prod and dev modes
CSV_FILE = os.environ.get("CSV_FILE", "backbone_locations.csv")
STRATEGIC_FILE = "strategic_analysis.json"

def generate_map():
    if not os.path.exists(CSV_FILE):
        print(f"❌ {CSV_FILE} not found.")
        return

    # 1. Load Strategic Intelligence (Universal Score Map)
    score_map = {}
    recommendation = None
    if os.path.exists(STRATEGIC_FILE):
        try:
            with open(STRATEGIC_FILE, 'r') as f:
                strategy = json.load(f)
                recommendation = strategy.get("strategic_recommendation")
                score_map = strategy.get("full_score_map", {})
        except Exception as e:
            print(f"⚠️ Could not load strategy JSON: {e}")

    # 2. Load and clean data
    df = pd.read_csv(CSV_FILE)
    
    # Defensive cleaning for accurate filtering and display
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce').fillna(0)
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').fillna(0)
    df['avg_rating'] = pd.to_numeric(df['avg_rating'], errors='coerce').fillna(0)
    df['num_places'] = pd.to_numeric(df['num_places'], errors='coerce').fillna(0).astype(int)
    df['total_reviews'] = pd.to_numeric(df['total_reviews'], errors='coerce').fillna(0).astype(int)

    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        print("⚠️ No valid data found.")
        return

    # Dynamic limits for sliders
    max_p_limit = int(df_clean['num_places'].max()) if not df_clean.empty else 100
    max_r_limit = int(df_clean['total_reviews'].max()) if not df_clean.empty else 500

    # 3. Initialize Map
    m = folium.Map(location=[38.0, -8.5], zoom_start=8, tiles="cartodbpositron")
    m.get_root().header.add_child(folium.Element('<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>'))

    marker_layer = folium.FeatureGroup(name="MainPropertyLayer")
    marker_layer.add_to(m)
    layer_var = marker_layer.get_name()

    prop_types = sorted(df_clean['location_type'].unique().tolist())

    for _, row in df_clean.iterrows():
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
        parking_display = f"{p_min} - {p_max}" if p_min != p_max else p_min

        # Parse Seasonality for Winter Stability
        seasonality_text = "No data"
        stability_ratio = 0.0
        try:
            if pd.notna(row.get('review_seasonality')):
                s_dict = json.loads(row['review_seasonality'])
                sorted_keys = sorted(s_dict.keys())
                seasonality_text = ", ".join([f"{k}: {s_dict[k]}" for k in sorted_keys[-2:]])
                winter_count = sum(v for k, v in s_dict.items() if any(m in k for m in ["-11", "-12", "-01", "-02"]))
                stability_ratio = 1.0 if winter_count > 0 else 0.0
        except: pass
        
        opp_score = score_map.get(str(row['p4n_id']), 0)
        
        if opp_score >= 85:
            marker_color, icon_type = 'cadetblue', 'star'
        elif opp_score >= 60:
            marker_color, icon_type = 'green', 'thumbs-up'
        else:
            marker_color, icon_type = 'orange', 'home'

        popup_html = f"""<div style="font-family: Arial; width: 320px; font-size: 13px;">
            <div style="float: right; background: {'#f1c40f' if opp_score >= 85 else '#eee'}; padding: 4px; border-radius: 4px; font-weight: bold;">
                Score: {opp_score if opp_score > 0 else 'N/A'}
            </div>
            <h3 style="margin-bottom: 5px; margin-top: 0;">{row['title']}</h3>
            <div style="color: #666; font-style: italic; margin-bottom: 10px;">{row['location_type']}</div>
            <b>FIRE Stats:</b> {num_places} places | <b>Rating:</b> {row['avg_rating']}⭐ ({row['total_reviews']} revs)<br>
            <b>Costs:</b> {parking_display} | <b>Elec:</b> {elec}<br>
            <b>Demographics:</b> {row.get('top_languages', 'N/A')}<br>
            <b>Winter Stability:</b> {'✅ STABLE' if stability_ratio > 0 else '❌ SEASONAL'}<br>
            <span style="font-size: 10px; color: #888;">Recent: {seasonality_text}</span>
            <div style="margin-top: 10px; border-top: 1px solid #eee; padding-top: 10px;">
                <b style="color: green;">Growth Moats (Pros):</b><br>
                <span style="font-size: 11px;">{row.get('ai_pros', 'None listed')}</span>
            </div>
            <div style="margin-top: 5px;">
                <b style="color: #d35400;">Yield Risks (Cons):</b><br>
                <span style="font-size: 11px;">{row.get('ai_cons', 'None listed')}</span>
            </div>
            <br><a href="{row['url']}" target="_blank" style="display: block; text-align: center; background: #2c3e50; color: white; padding: 8px; border-radius: 4px; text-decoration: none; font-weight: bold;">View Data Source</a>
        </div>"""

        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=350),
            icon=folium.Icon(color=marker_color, icon=icon_type, prefix='fa')
        )
        
        # EMBED DATA FOR JAVASCRIPT: Added total_reviews for the new filter
        marker.options['extraData'] = {
            'rating': float(row['avg_rating']),
            'places': int(num_places),
            'reviews': int(row['total_reviews']),
            'type': str(row['location_type']),
            'score': opp_score,
            'seasonality': row['review_seasonality'] if pd.notna(row['review_seasonality']) else "{}",
            'pros': row['ai_pros'] if pd.notna(row['ai_pros']) else "",
            'cons': row['ai_cons'] if pd.notna(row['ai_cons']) else ""
        }
        marker.add_to(marker_layer)

    # 4. UI AND INTERACTIVE LOGIC
    strat_box = f"""
    <div id="strat-panel" class="map-overlay" style="bottom: 20px; left: 20px; width: 280px; border-left: 5px solid #f1c40f;">
        <h4 style="margin:0; color: #2c3e50;">🔥 FIRE Investment Memo</h4>
        <hr style="margin: 10px 0;">
        <div style="font-size: 12px;">
            <b>Target Region:</b> {recommendation['target_region'] if recommendation else 'Awaiting Analysis...'}<br>
            <b>Max Opportunity:</b> <span style="color: #27ae60; font-weight: bold;">{recommendation['opportunity_score'] if recommendation else 'N/A'} pts</span><br>
            <p style="margin-top: 8px; font-style: italic;">"{recommendation['market_gap'] if recommendation else 'Recalculating...'}"</p>
        </div>
    </div>
    """

    ui_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; padding: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: fixed; z-index: 9999; overflow-y: auto; }}
        #filter-panel {{ top: 20px; right: 20px; width: 220px; max-height: 90vh; }}
        #stats-panel {{ top: 20px; left: 20px; width: 320px; max-height: 70vh; }}
        .stat-section {{ margin-top: 12px; border-top: 1px solid #eee; padding-top: 8px; font-size: 11px; }}
        .tag-item {{ display: flex; justify-content: space-between; margin-bottom: 2px; }}
        .tag-item b {{ color: #2c3e50; }}
        .btn-apply {{ background: #2c3e50; color: white; width: 100%; padding: 10px; border-radius: 6px; cursor: pointer; font-weight: bold; border: none; margin-top: 10px; }}
        .btn-reset {{ background: #95a5a6; color: white; width: 100%; padding: 10px; border-radius: 6px; cursor: pointer; font-weight: bold; border: none; margin-top: 5px; }}
    </style>

    {strat_box}

    <div id="stats-panel" class="map-overlay">
        <h4 style="margin:0;">📊 Market Intelligence</h4>
        <p style="font-size: 11px; color: #666; margin-bottom: 4px;">Aggregating <span id="agg-count">{len(df_clean)}</span> visible sites</p>
        <p style="font-size: 11px; color: #2c3e50; margin-top: 0;"><b>Total Reviews (Since 2024):</b> <span id="recent-review-count">0</span></p>
        
        <div class="stat-section">
            <b>Review Seasonality (Total)</b>
            <canvas id="seasonChart" height="150"></canvas>
        </div>
        <div class="stat-section">
            <b style="color: green;">Top 10 Pros</b>
            <div id="top-pros" style="margin-top:5px;"></div>
        </div>
        <div class="stat-section">
            <b style="color: #d35400;">Top 10 Cons</b>
            <div id="top-cons" style="margin-top:5px;"></div>
        </div>
    </div>

    <div id="filter-panel" class="map-overlay">
        <h3 style="margin:0;">Filters</h3>
        <p style="font-size: 12px;">Sites matching: <b id="match-count">{len(df_clean)}</b></p>
        
        <div class="stat-section">
            <b>Min Rating: <span id="txt-rating">0</span></b>
            <input type="range" id="range-rating" min="0" max="5" step="0.1" value="0" style="width:100%" oninput="document.getElementById('txt-rating').innerText=this.value">
        </div>

        <div class="stat-section">
            <b>Min Places: <span id="txt-places">0</span></b>
            <input type="range" id="range-places" min="0" max="{max_p_limit}" step="1" value="0" style="width:100%" oninput="document.getElementById('txt-places').innerText=this.value">
        </div>

        <div class="stat-section">
            <b>Max Places: <span id="txt-max-places">{max_p_limit}</span></b>
            <input type="range" id="range-max-places" min="0" max="{max_p_limit}" step="1" value="{max_p_limit}" style="width:100%" oninput="document.getElementById('txt-max-places').innerText=this.value">
        </div>

        <div class="stat-section">
            <b>Max Total Reviews: <span id="txt-max-revs">{max_r_limit}</span></b>
            <input type="range" id="range-max-revs" min="0" max="{max_r_limit}" step="5" value="{max_r_limit}" style="width:100%" oninput="document.getElementById('txt-max-revs').innerText=this.value">
        </div>
        
        <select id="sel-type" style="width:100%; margin-top:10px;">
            <option value="All">All Types</option>
            {" ".join([f'<option value="{t}">{t}</option>' for t in prop_types])}
        </select>
        
        <button onclick="applyFilters()" class="btn-apply">Apply Filters</button>
        <button onclick="resetFilters()" class="btn-reset">Reset</button>
    </div>

    <script>
    var markerStore = null;
    var chartInstance = null;

    function parseThemeString(str) {{
        const results = {{}};
        if (!str) return results;
        str.split(';').forEach(item => {{
            const match = item.match(/(.+)\\s\\((\\d+)\\)/);
            if (match) {{ results[match[1].trim()] = parseInt(match[2]); }}
        }});
        return results;
    }}

    function updateDashboard(activeMarkers) {{
        let globalSeason = {{}};
        let globalPros = {{}};
        let globalCons = {{}};
        let totalRecentReviews = 0;

        activeMarkers.forEach(m => {{
            const d = m.options.extraData;
            try {{
                const s = JSON.parse(d.seasonality);
                for (let date in s) {{
                    const month = date.split('-')[1];
                    globalSeason[month] = (globalSeason[month] || 0) + s[date];
                    if (date >= "2024-01") {{ totalRecentReviews += s[date]; }}
                }}
            }} catch(e) {{}}
            const p = parseThemeString(d.pros);
            for (let k in p) {{ globalPros[k] = (globalPros[k] || 0) + p[k]; }}
            const c = parseThemeString(d.cons);
            for (let k in c) {{ globalCons[k] = (globalCons[k] || 0) + c[k]; }}
        }});

        document.getElementById('recent-review-count').innerText = totalRecentReviews.toLocaleString();
        document.getElementById('agg-count').innerText = activeMarkers.length;

        const labels = ["01","02","03","04","05","06","07","08","09","10","11","12"];
        const ctx = document.getElementById('seasonChart').getContext('2d');
        if (chartInstance) chartInstance.destroy();
        chartInstance = new Chart(ctx, {{
            type: 'bar',
            data: {{
                labels: labels,
                datasets: [{{ label: 'Reviews', data: labels.map(l => globalSeason[l] || 0), backgroundColor: '#3498db' }}]
            }},
            options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
        }});

        const renderTop10 = (data, divId) => {{
            const sorted = Object.entries(data).sort((a,b) => b[1]-a[1]).slice(0, 10);
            document.getElementById(divId).innerHTML = sorted.map(i => 
                `<div class="tag-item"><span>${{i[0]}}</span><b>${{i[1]}}</b></div>`).join('');
        }};
        renderTop10(globalPros, 'top-pros');
        renderTop10(globalCons, 'top-cons');
    }}

    function applyFilters() {{
        const minR = parseFloat(document.getElementById('range-rating').value);
        const minP = parseInt(document.getElementById('range-places').value);
        const maxP = parseInt(document.getElementById('range-max-places').value);
        const maxRev = parseInt(document.getElementById('range-max-revs').value);
        const type = document.getElementById('sel-type').value;

        var targetLayer = window['{layer_var}'];
        if (!markerStore) markerStore = targetLayer.getLayers();

        targetLayer.clearLayers();
        const filtered = markerStore.filter(m => {{
            const d = m.options.extraData;
            return d.rating >= minR && 
                   d.places >= minP && 
                   d.places <= maxP && 
                   d.reviews <= maxRev && 
                   (type === "All" || d.type === type);
        }});

        filtered.forEach(m => targetLayer.addLayer(m));
        document.getElementById('match-count').innerText = filtered.length;
        updateDashboard(filtered);
    }}

    function resetFilters() {{
        document.getElementById('range-rating').value = 0;
        document.getElementById('txt-rating').innerText = 0;
        document.getElementById('range-places').value = 0;
        document.getElementById('txt-places').innerText = 0;
        document.getElementById('range-max-places').value = {max_p_limit};
        document.getElementById('txt-max-places').innerText = {max_p_limit};
        document.getElementById('range-max-revs').value = {max_r_limit};
        document.getElementById('txt-max-revs').innerText = {max_r_limit};
        document.getElementById('sel-type').value = "All";
        applyFilters();
    }}
    
    window.onload = () => {{
        setTimeout(() => {{
            var layer = window['{layer_var}'];
            updateDashboard(layer.getLayers());
        }}, 1000);
    }};
    </script>
    """
    m.get_root().html.add_child(folium.Element(ui_html))
    m.save("index.html")
    print(f"🚀 Map generated with Recent Reviews and Max Review/Places filters.")

if __name__ == "__main__":
    generate_map()
