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

    # 1. Load Strategic Intelligence
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

    df = pd.read_csv(CSV_FILE)
    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        return

    m = folium.Map(location=[38.0, -8.5], zoom_start=8, tiles="cartodbpositron")
    
    # Add Chart.js to the Header
    m.get_root().header.add_child(folium.Element('<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>'))

    marker_layer = folium.FeatureGroup(name="MainPropertyLayer")
    marker_layer.add_to(m)
    layer_name = marker_layer.get_name()

    for _, row in df_clean.iterrows():
        opp_score = score_map.get(str(row['p4n_id']), 0)
        
        # Standard Marker Creation Logic
        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(f"<b>{row['title']}</b>", max_width=350),
            icon=folium.Icon(color='orange' if opp_score < 60 else 'green', icon='home', prefix='fa')
        )
        
        # DATA EMBEDDING: Store raw values for JS aggregation
        marker.options['extraData'] = {
            'rating': float(row['avg_rating']),
            'places': int(row.get('num_places', 0)),
            'type': str(row['location_type']),
            'seasonality': row['review_seasonality'] if pd.notna(row['review_seasonality']) else "{}",
            'pros': row['ai_pros'] if pd.notna(row['ai_pros']) else "",
            'cons': row['ai_cons'] if pd.notna(row['ai_cons']) else ""
        }
        marker.add_to(marker_layer)

    # 2. INTERACTIVE DASHBOARD HTML
    dashboard_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; padding: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: fixed; z-index: 9999; overflow-y: auto; }}
        #filter-panel {{ top: 20px; right: 20px; width: 240px; }}
        #stats-panel {{ top: 20px; left: 20px; width: 350px; max-height: 80vh; }}
        .stat-section {{ margin-bottom: 20px; border-top: 1px solid #eee; padding-top: 10px; }}
        .tag-list {{ font-size: 11px; }}
        .tag-item {{ display: flex; justify-content: space-between; margin-bottom: 3px; }}
    </style>

    <div id="stats-panel" class="map-overlay">
        <h4 style="margin:0;">📈 Market Intelligence</h4>
        <p style="font-size: 11px; color: #666;">Aggregated from <span id="agg-count">{len(df_clean)}</span> visible sites</p>
        
        <div class="stat-section">
            <h6 style="font-size: 12px; font-weight: bold;">Review Seasonality</h6>
            <canvas id="seasonChart" height="150"></canvas>
        </div>

        <div class="stat-section">
            <h6 style="font-size: 12px; font-weight: bold; color: green;">Top 10 Growth Moats (Pros)</h6>
            <div id="top-pros" class="tag-list"></div>
        </div>

        <div class="stat-section">
            <h6 style="font-size: 12px; font-weight: bold; color: #d35400;">Top 10 Yield Risks (Cons)</h6>
            <div id="top-cons" class="tag-list"></div>
        </div>
    </div>

    <script>
    var markerStore = null;
    var chartInstance = null;

    function parseTags(dataString) {{
        const tags = {{}};
        if (!dataString) return tags;
        dataString.split(';').forEach(item => {{
            const match = item.match(/(.+)\\s\\((\\d+)\\)/);
            if (match) {{
                const topic = match[1].trim();
                const count = parseInt(match[2]);
                tags[topic] = (tags[topic] || 0) + count;
            }}
        }});
        return tags;
    }}

    function updateStats(filteredMarkers) {{
        let seasonData = {{}};
        let totalPros = {{}};
        let totalCons = {{}};

        filteredMarkers.forEach(m => {{
            const d = m.options.extraData;
            
            // Aggregate Seasonality
            try {{
                const s = JSON.parse(d.seasonality);
                for (let key in s) {{ seasonData[key] = (seasonData[key] || 0) + s[key]; }}
            }} catch(e) {{}}

            // Aggregate Pros/Cons
            const p = parseTags(d.pros);
            for (let k in p) {{ totalPros[k] = (totalPros[k] || 0) + p[k]; }}
            const c = parseTags(d.cons);
            for (let k in c) {{ totalCons[k] = (totalCons[k] || 0) + c[k]; }}
        }});

        // Update Seasonality Histogram
        const sortedMonths = Object.keys(seasonData).sort();
        const ctx = document.getElementById('seasonChart').getContext('2d');
        if (chartInstance) chartInstance.destroy();
        chartInstance = new Chart(ctx, {{
            type: 'bar',
            data: {{
                labels: sortedMonths.map(m => m.substring(5)), // Show MM only
                datasets: [{{ label: 'Reviews', data: sortedMonths.map(m => seasonData[m]), backgroundColor: '#3498db' }}]
            }},
            options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
        }});

        // Render Top 10 Lists
        const renderList = (data, elementId) => {{
            const sorted = Object.entries(data).sort((a,b) => b[1] - a[1]).slice(0, 10);
            document.getElementById(elementId).innerHTML = sorted.map(i => 
                `<div class="tag-item"><span>${{i[0]}}</span><b>${{i[1]}}</b></div>`).join('');
        }};
        renderList(totalPros, 'top-pros');
        renderList(totalCons, 'top-cons');
        document.getElementById('agg-count').innerText = filteredMarkers.length;
    }}

    function applyFilters() {{
        const minR = parseFloat(document.getElementById('range-rating').value);
        const minP = parseInt(document.getElementById('range-places').value);
        const type = document.getElementById('sel-type').value;

        var targetLayer = window['{layer_name}'];
        if (!markerStore) markerStore = targetLayer.getLayers();

        targetLayer.clearLayers();
        const filtered = markerStore.filter(m => {{
            const d = m.options.extraData;
            return d.rating >= minR && d.places >= minP && (type === "All" || d.type === type);
        }});

        filtered.forEach(m => targetLayer.addLayer(m));
        document.getElementById('match-count').innerText = filtered.length;
        
        // RECALCULATE AGGREGATES
        updateStats(filtered);
    }}
    
    // Initial load
    window.onload = () => {{
        setTimeout(() => {{
            var targetLayer = window['{layer_name}'];
            updateStats(targetLayer.getLayers());
        }}, 1000);
    }};
    </script>
    """
    m.get_root().html.add_child(folium.Element(dashboard_html))
    m.save("index.html")
