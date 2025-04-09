import pandas as pd
from decimal import Decimal
import json
import streamlit as st
import requests
from datetime import datetime, timedelta
from helpers.connection import get_cache_db_connection, get_main_db_connection
from helpers.constants import CHAIN_ID_MAP

def fetch_timeseries(metric, start_date=None, end_date=None):
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT date, value FROM timeseries_cache WHERE metric = %s"
                params = [metric]
                if start_date:
                    query += " AND date >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND date <= %s"
                    params.append(end_date)
                cursor.execute(query, tuple(params))
                df = pd.DataFrame(cursor.fetchall(), columns=["date", "value"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception:
        return pd.DataFrame()

def fetch_timeseries_chain_volume(metric="swap_volume", chains=None, status="success"):
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT date, SUM(value) as value, SUM(quantity) as quantity
                    FROM timeseries_chain_volume
                    WHERE metric = %s AND status = %s
                """
                params = [metric, status]
                if chains:
                    placeholders = ','.join(['%s'] * len(chains))
                    query += f" AND chain IN ({placeholders})"
                    params.extend(chains)
                query += " GROUP BY date ORDER BY date ASC"
                cursor.execute(query, tuple(params))
                df = pd.DataFrame(cursor.fetchall(), columns=["date", "value", "quantity"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception:
        return pd.DataFrame()

def fetch_swap_series():
    try:
        with get_main_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT "createdAt", transaction, "chainIds"
                    FROM public."Activity"
                    WHERE status = 'SUCCESS' AND type = 'SWAP'
                """)
                rows = cursor.fetchall()
                swap_data = {}
                for created_at, txn_raw, chain_ids in rows:
                    try:
                        txn = json.loads(txn_raw)
                        if not chain_ids or len(chain_ids) < 1:
                            continue
                        from_chain_id = chain_ids[0]
                        chain_name = CHAIN_ID_MAP.get(from_chain_id, str(from_chain_id))

                        from_amount = txn.get('fromAmount') or txn.get('amount') or txn.get('value') or 0
                        from_amount = Decimal(from_amount)

                        from_token = txn.get('fromToken') or txn.get('token') or txn.get('sourceToken') or {}
                        decimals = int(from_token.get('decimals') or from_token.get('decimal') or 18)

                        price_usd = from_token.get('tokenPrices', {}).get('usd') or \
                                    from_token.get('price', {}).get('usd') or \
                                    from_token.get('priceUSD') or 0
                        price_usd = Decimal(price_usd)

                        if from_amount <= 0 or price_usd <= 0:
                            continue
                        if from_amount > 1e50:
                            from_amount = Decimal('1e50')
                        if price_usd > 1e6:
                            price_usd = Decimal('1e6')

                        normalized = from_amount / Decimal(10 ** decimals)
                        volume = float(normalized * price_usd)
                        if volume > 1e308:
                            continue

                        date_str = created_at.date().isoformat()
                        key = (date_str, chain_name, "swap_volume", "success")
                        if key in swap_data:
                            swap_data[key]["value"] += volume
                            swap_data[key]["quantity"] += 1
                        else:
                            swap_data[key] = {
                                "date": date_str,
                                "chain": chain_name,
                                "metric": "swap_volume",
                                "status": "success",
                                "value": volume,
                                "quantity": 1
                            }
                    except Exception:
                        continue
                return list(swap_data.values())
    except Exception:
        return []

HEADERS = {
    "Authorization": f"Basic {st.secrets['api']['AUTH_KEY']}"
}
ENDPOINTS = dict(st.secrets["api"])

def fetch_api_metric(key, start=None, end=None):
    url = ENDPOINTS[key]
    params = {}
    if start:
        params["start"] = start
        if not end:
            if key == "cash_volume":
                end = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                end = start
        params["end"] = end
    try:
        res = requests.get(url, headers=HEADERS, params=params)
        res.raise_for_status()
        data = res.json()
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict) and "value" in data:
            return pd.DataFrame([{"date": start, "value": float(data["value"])}])
        return pd.DataFrame([{"date": start, "value": float(data)}])
    except Exception:
        return pd.DataFrame()
