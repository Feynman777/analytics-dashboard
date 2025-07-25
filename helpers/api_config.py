import os
from urllib.parse import urljoin

# === Base API URL from env or default ===
API_BASE_URL = os.getenv("API_BASE_URL", "https://newmoney-ai-analytics-prod-production.up.railway.app/")

# === Constructed API endpoints ===
cash_volume        = urljoin(API_BASE_URL, "user/cash/volume")
active_users       = urljoin(API_BASE_URL, "user/active")
new_users          = urljoin(API_BASE_URL, "user/new")
referrals          = urljoin(API_BASE_URL, "user/referrals")
total_users        = urljoin(API_BASE_URL, "user/total")
total_agents       = urljoin(API_BASE_URL, "agents/deployed")
user_full_metrics  = urljoin(API_BASE_URL, "user/metrics/")
user_volume        = urljoin(API_BASE_URL, "user/metrics/volume/")
user_agents        = urljoin(API_BASE_URL, "agents/user/")

# === API auth key ===
AUTH_KEY = os.getenv("AUTH_KEY", "missing-auth-key")

headers = {
    "Authorization": f"Basic {AUTH_KEY}",
    "Content-Type": "application/json"
}
