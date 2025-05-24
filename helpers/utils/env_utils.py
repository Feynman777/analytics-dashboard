import os

try:
    import streamlit as st
except ImportError:
    st = None

def get_env_or_secret(key, section=None, default=None):
    # Try environment variable first
    if os.getenv(key):
        return os.getenv(key)

    # Then try Streamlit secrets
    if st and hasattr(st, "secrets"):
        if section:
            return st.secrets.get(section, {}).get(key, default)
        return st.secrets.get(key, default)

    # Fallback to provided default
    return default
