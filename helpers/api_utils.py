import requests
from urllib.parse import urljoin
from utils.env_utils import get_env_or_secret

# === Load from secrets or env
API_BASE_URL = get_env_or_secret("API_BASE_URL")
AUTH_KEY = get_env_or_secret("AUTH_KEY", default="dev-auth-key")

headers = {
    "Authorization": f"Basic {AUTH_KEY}",
    "Content-Type": "application/json"
}

def fetch_api_metric(endpoint: str, start: str = None, end: str = None):
    url = urljoin(API_BASE_URL, endpoint)
    params = {}
    if start: params["start"] = start
    if end: params["end"] = end
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ API fetch failed: {e}")
        return {}
