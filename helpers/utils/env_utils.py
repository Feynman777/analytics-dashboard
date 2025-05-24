import os

try:
    from streamlit import secrets
except ImportError:
    secrets = None

def get_env_or_secret(key, section=None, default=None):
    # Prefer env variable first (with section prefix if applicable)
    env_key = f"{section.upper()}_{key}" if section else key
    if os.getenv(env_key):
        return os.getenv(env_key)

    # Fall back to streamlit secrets (if running in Streamlit)
    if secrets:
        if section:
            return secrets.get(section, {}).get(key, default)
        return secrets.get(key, default)

    return default