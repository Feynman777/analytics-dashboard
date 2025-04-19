import pandas as pd
import requests
from datetime import datetime, timedelta
import streamlit as st

HEADERS = {
    "Authorization": f"Basic {st.secrets['api']['AUTH_KEY']}"
}
ENDPOINTS = dict(st.secrets["api"])

def fetch_api_metric(key, start=None, end=None, username=None):
    url = ENDPOINTS[key]

    # Inject username into the URL path if applicable
    if username and "{username}" not in url:
        if url.endswith("/"):
            url += username
        else:
            url += f"/{username}"

    params = {}
    if start:
        params["start"] = start
        if not end:
            if key == "cash_volume":
                end = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                end = start
        params["end"] = end

    try:
        res = requests.get(url, headers=HEADERS, params=params)
        res.raise_for_status()
        data = res.json()

        # Handle dict response like {"volume": 208.19, "qty": 13}
        if isinstance(data, dict):
            # Special case for full user profile with nested keys
            if key == "user_full_metrics":
                return data
            return pd.DataFrame([{"date": start, **data}])

        # Handle array-style responses
        elif isinstance(data, list):
            return pd.DataFrame(data)

        # Catch-all fallback
        return pd.DataFrame([{"date": start, "value": float(data)}])

    except Exception as e:
        print(f"[ERROR] fetch_api_metric failed: {e}")
        return pd.DataFrame() 