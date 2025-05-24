# === cron_sync_apps.py ===
import os
import base64

# Decode base64 key from env var (set in GitHub secrets)
if "BQ_KEY_BASE64" in os.environ:
    key_path = "/tmp/firebase-bq-key.json"
    with open(key_path, "wb") as f:
        f.write(base64.b64decode(os.environ["BQ_KEY_BASE64"]))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path