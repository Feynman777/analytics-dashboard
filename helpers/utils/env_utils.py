import os

try:
    from streamlit import secrets
except ImportError:
    secrets = None

def get_env_or_secret(key, section=None, default=None):
    # Try environment variable first
    if os.getenv(key):
        return os.getenv(key)

    # Then try Streamlit secrets
    if secrets:
        if section:
            return secrets.get(section, {}).get(key, default)
        return secrets.get(key, default)

    # Fallback to provided default
    return default
