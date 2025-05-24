import os

try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False

def get_env_or_secret(key, section=None, default=None):
    # Try environment variable first (GitHub Actions and local testing)
    if key in os.environ:
        return os.environ[key]

    # Try Streamlit secrets (Streamlit Cloud runtime)
    if HAS_STREAMLIT and hasattr(st, "secrets"):
        try:
            if section:
                return st.secrets.get(section, {}).get(key, default)
            return st.secrets.get(key, default)
        except Exception:
            pass

    # Fallback to default
    return default
