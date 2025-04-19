import requests
from streamlit import secrets

AUTH_KEY = secrets["api"]["AUTH_KEY"]
API_BASE_URL = secrets["api"]["API_BASE_URL"]

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
