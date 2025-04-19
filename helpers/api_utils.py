import os
import requests

# Try Streamlit secrets if available, else fallback to environment variables
try:
    from streamlit import secrets
    AUTH_KEY = secrets["AUTH_KEY"]
    API_BASE_URL = secrets["API_BASE_URL"]
except Exception:
    AUTH_KEY = os.getenv("AUTH_KEY", "dev-default-key")
    API_BASE_URL = os.getenv("API_BASE_URL", "https://newmoney-ai-analytics-prod-production.up.railway.app/")

headers = {
    "Authorization": f"Basic {AUTH_KEY}",
    "Content-Type": "application/json"
}

def fetch_api_metric(endpoint: str, date: str = None):
    url = f"{API_BASE_URL}{endpoint}"
    if date:
        url = f"{url}?date={date}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ API fetch failed: {e}")
        return {}
