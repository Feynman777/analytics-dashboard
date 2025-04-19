import os
import requests
from urllib.parse import urljoin

# === Load base URL and auth key from environment ===
API_BASE_URL = os.getenv("API_BASE_URL", "https://newmoney-ai-analytics-prod-production.up.railway.app/")
AUTH_KEY = os.getenv("AUTH_KEY", "dev-default-key")

# === Common request headers ===
HEADERS = {
    "Authorization": f"Basic {AUTH_KEY}",
    "Content-Type": "application/json",
}

def fetch_api_metric(metric: str, date: str = None):
    """
    Fetch a single-day or full-metric timeseries from the analytics API.

    Args:
        metric (str): e.g., "cash_volume", "referrals", "total_agents"
        date (str): optional ISO string like '2025-04-15'

    Returns:
        pd.DataFrame: The parsed API response as a dataframe
    """
    import pandas as pd

    path = f"timeseries/{metric}"
    if date:
        path += f"?date={date}"

    url = urljoin(API_BASE_URL, path)

    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"❌ Failed to fetch {metric}: {e}")
        return pd.DataFrame()
