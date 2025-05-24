import requests
from typing import Tuple
import streamlit as st

api_url = st.secrets["cash"]["yield_api_url"]

def fetch_cash_yield_metrics() -> Tuple[float, float]:
    """
    Fetches the latest cash yield metrics.
    Returns:
        - lifetime_yield: Total yield since inception (balance - original_balance summed across all assets).
        - yield_24h: Delta in total yield from previous to latest timestamp across all assets.
    """
    try:
        print(f"📡 Requesting cash yield from: {api_url}")
        response = requests.get(api_url)
        print("➡️ Response status code:", response.status_code)
        print("📦 Raw response text (truncated):", response.text[:500])

        response.raise_for_status()
        data = response.json()
        print("✅ Parsed JSON keys:", list(data.keys()))

        # === Lifetime Yield ===
        fullassets = data.get("fullassets", {})
        lifetime_yield = sum(
            float(asset.get("balance", 0)) - float(asset.get("original_balance", 0))
            for asset in fullassets.values()
        )
        print("📊 Calculated lifetime_yield:", lifetime_yield)

        # === 24h Yield from assethistory ===
        assethistory = data.get("assethistory", {})
        print(f"🕓 Found assethistory for {len(assethistory)} assets")

        yield_24h = 0.0
        for asset_id, entries in assethistory.items():
            if len(entries) >= 2:
                prev = entries[-2]
                latest = entries[-1]
                prev_yield = float(prev[1]) - float(prev[2])  # balance - original
                latest_yield = float(latest[1]) - float(latest[2])
                delta = latest_yield - prev_yield
                yield_24h += delta
                print(f"↪️ {asset_id}: Δ={delta:.4f} ({latest_yield:.4f} - {prev_yield:.4f})")

        print("📊 Calculated yield_24h:", yield_24h)

        return lifetime_yield, yield_24h

    except Exception as e:
        print(f"❌ Exception in fetch_cash_yield_metrics: {e}")
        return 0.0, 0.0
