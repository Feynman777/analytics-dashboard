import os

# Only import streamlit if available, otherwise fallback
try:
    import streamlit as st
    _secrets_available = True
except ImportError:
    st = None
    _secrets_available = False

def get_env_or_secret(key, section=None, default=None):
    # 1. Try environment variable first
    if os.getenv(key):
        return os.getenv(key)

    # 2. Try Streamlit secrets only if available
    if _secrets_available and st.secrets:
        try:
            if section:
                return st.secrets.get(section, {}).get(key, default)
            return st.secrets.get(key, default)
        except Exception:
            pass  # Streamlit secrets file missing

    # 3. Fallback
    return default