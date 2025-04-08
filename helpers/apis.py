#helpers/apis.py
import requests
import streamlit as st
from datetime import datetime, timezone, timedelta
import pandas as pd

HEADERS = {
    "Authorization": f"Basic {st.secrets['api']['AUTH_KEY']}"
}
ENDPOINTS = dict(st.secrets["api"])

def fetch_metric_value(key, start=None, end=None):
    """
    Fetch metric value from an API endpoint for a given date range.
    
    Args:
        key (str): The API endpoint key from ENDPOINTS.
        start (str, optional): Start date in YYYY-MM-DD format.
        end (str, optional): End date in YYYY-MM-DD format. Defaults based on key.
    
    Returns:
        pd.DataFrame: DataFrame with date and value columns, or empty DataFrame on error.
    """
    url = ENDPOINTS[key]
    params = {}
    if start:
        params["start"] = start
        if not end:
            if key == "cash_volume":
                # Special case for cash_volume: end = start + 1
                start_date = datetime.strptime(start, "%Y-%m-%d")
                end_date = start_date + timedelta(days=1)
                params["end"] = end_date.strftime("%Y-%m-%d")
            else:
                # Default for others: end = start
                params["end"] = start
        else:
            params["end"] = end
    try:
        print(f"[DEBUG] Fetching: {url} with params {params}")
        res = requests.get(url, headers=HEADERS, params=params)
        res.raise_for_status()
        data = res.json()
        print(f"[DEBUG] Raw API response for {key}: {data}")
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict) and "value" in data:
            return pd.DataFrame([{"date": start, "value": float(data["value"])}])
        return pd.DataFrame([{"date": start, "value": float(data)}])
    except Exception as e:
        st.error(f"Error fetching {key}: {e}")
        return pd.DataFrame()