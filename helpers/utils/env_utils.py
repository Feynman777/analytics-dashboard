import os

def get_env_or_secret(key, section=None, default=None):
    # 1. Check environment variable
    if os.getenv(key):
        return os.getenv(key)

    # 2. Check Streamlit secrets only if inside Streamlit
    try:
        import streamlit as st
        if section:
            return st.secrets.get(section, {}).get(key, default)
        return st.secrets.get(key, default)
    except (ImportError, AttributeError):
        pass

    # 3. Fallback default
    return default