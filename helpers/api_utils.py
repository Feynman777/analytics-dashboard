import pandas as pd
import requests
from urllib.parse import urljoin
from helpers.utils.env_utils import get_env_or_secret

API_BASE_URL = get_env_or_secret("API_BASE_URL", section="api")
AUTH_KEY = get_env_or_secret("AUTH_KEY", section="api", default="dev-auth-key")

headers = {
    "Authorization": f"Basic {AUTH_KEY}"
}

def fetch_api_raw(endpoint: str, params: dict = None) -> str:
    base = API_BASE_URL if API_BASE_URL.endswith("/") else API_BASE_URL + "/"
    url = urljoin(base, endpoint)

    try:
        response = requests.get(url, headers=headers, params=params or {})
        response.raise_for_status()
        return response.text  # raw number or plain string
    except Exception as e:
        print(f"❌ API RAW fetch failed: {e}\n↳ URL: {url} | Params: {params}")
        return "0"

def fetch_api_metric(endpoint: str, start: str = None, end: str = None, username: str = None) -> pd.DataFrame:
    base = API_BASE_URL if API_BASE_URL.endswith("/") else API_BASE_URL + "/"
    url = urljoin(base, endpoint)

    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if username:
        params["username"] = username

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        json_data = response.json()

        if isinstance(json_data, list):
            return pd.DataFrame(json_data)
        elif isinstance(json_data, dict):
            return pd.DataFrame([json_data])
        elif isinstance(json_data, (int, float)):
            return pd.DataFrame([{"value": json_data}])

        print(f"⚠️ Unexpected API response format for {url}: {json_data}")
        return pd.DataFrame()

    except Exception as e:
        print(f"❌ API fetch failed: {e}\n↳ URL: {url} | Params: {params}")
        return pd.DataFrame()
    

def fetch_api_json(endpoint: str, params: dict = None) -> dict:
    base = API_BASE_URL if API_BASE_URL.endswith("/") else API_BASE_URL + "/"
    url = urljoin(base, endpoint)

    try:
        response = requests.get(url, headers=headers, params=params or {})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ API JSON fetch failed: {e}\n↳ URL: {url} | Params: {params}")
        return {}
