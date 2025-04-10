import pandas as pd
from decimal import Decimal
import json
import streamlit as st
import requests
from datetime import date, datetime, timedelta
from helpers.connection import get_cache_db_connection, get_main_db_connection
from helpers.constants import CHAIN_ID_MAP
from helpers.upsert import upsert_avg_revenue_metrics


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
    
def fetch_cached_fees():
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT date, chain, value
                    FROM timeseries_fees
                    ORDER BY date ASC
                """)
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=["date", "chain", "value"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception as e:
        print(f"[ERROR] Failed to fetch cached fees: {e}")
        return pd.DataFrame()

def fetch_swap_series(start=None, end=None):
    try:
        with get_main_db_connection() as conn:
            with conn.cursor() as cursor:
                query = """
                    SELECT "createdAt", transaction, "chainIds"
                    FROM public."Activity"
                    WHERE status = 'SUCCESS' AND type = 'SWAP'
                """
                params = []
                if start:
                    query += " AND DATE(\"createdAt\") >= %s"
                    params.append(start)
                if end:
                    query += " AND DATE(\"createdAt\") <= %s"
                    params.append(end)

                cursor.execute(query, params)
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
    

def fetch_fee_series():
    fee_data = []

    with get_main_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT "createdAt", transaction, "chainIds"
                FROM public."Activity"
                WHERE status = 'SUCCESS' AND type = 'SWAP'
            """)
            rows = cursor.fetchall()

    for created_at, txn_raw, chain_ids in rows:
        try:
            txn = json.loads(txn_raw)

            # === Handle SUI-style fee ===
            nm_fee = txn.get("route", {}).get("nmFee", {})
            if nm_fee and "amount" in nm_fee:
                amount = Decimal(nm_fee.get("amount", 0))
                token = nm_fee.get("token", {})
                price_usd = Decimal(token.get("tokenPrices", {}).get("usd", 0))
                decimals = int(token.get("decimals", 18))

                if amount > 0 and price_usd > 0:
                    value_usd = float(amount * price_usd / Decimal(10 ** decimals))
                    fee_data.append({
                        "date": created_at.date().isoformat(),
                        "chain": chain_ids[0] if chain_ids else "unknown",
                        "value": value_usd
                    })

            # === Handle LIFI-style fee ===
            steps = txn.get("route", {}).get("steps", [])
            for step in steps:
                estimate = step.get("estimate", {})
                fee_costs = estimate.get("feeCosts", [])

                for fee in fee_costs:
                    amount = Decimal(fee.get("amount", 0))
                    token = fee.get("token", {})
                    price_usd = Decimal(token.get("priceUSD", 0))
                    decimals = int(token.get("decimals", 18))

                    if amount > 0 and price_usd > 0:
                        value_usd = float(amount * price_usd / Decimal(10 ** decimals))
                        fee_data.append({
                            "date": created_at.date().isoformat(),
                            "chain": chain_ids[0] if chain_ids else "unknown",
                            "value": value_usd
                        })

        except Exception:
            continue

    df = pd.DataFrame(fee_data)
    df["date"] = pd.to_datetime(df["date"])
    return df

def fetch_avg_revenue_metrics(days: int = 30) -> dict:
    """Fetch cached avg revenue metrics or calculate and store if missing."""
    snapshot_date = date.today()

    with get_main_db_connection() as conn_main, get_cache_db_connection() as conn_cache:
        cur_main = conn_main.cursor()
        cur_cache = conn_cache.cursor()

        # 1. Try to load today's snapshot from DB
        cur_cache.execute("SELECT * FROM avg_revenue_metrics WHERE date = %s", (snapshot_date,))
        row = cur_cache.fetchone()
        if row:
            return {
                "date": row[0],
                "total_fees": float(row[1] or 0),
                "total_users": row[2],
                "active_users": row[3],
                "avg_rev_per_user": float(row[4] or 0),
                "avg_rev_per_active_user": float(row[5] or 0)
            }

        # 2. Calculate if not found
        start_date = snapshot_date - timedelta(days=days)

        cur_cache.execute("SELECT SUM(value) FROM timeseries_fees WHERE date >= %s", (start_date,))
        total_fees = cur_cache.fetchone()[0] or 0

        cur_main.execute('SELECT COUNT(*) FROM "User" WHERE "createdAt" >= %s', (start_date,))
        total_users = cur_main.fetchone()[0] or 0

        cur_main.execute('''
            SELECT COUNT(DISTINCT "userId")
            FROM "Activity"
            WHERE type = 'SWAP' AND status = 'SUCCESS' AND "createdAt" >= %s
        ''', (start_date,))
        active_users = cur_main.fetchone()[0] or 0

        result = {
            "date": snapshot_date,
            "total_fees": total_fees,
            "total_users": total_users,
            "active_users": active_users,
            "avg_rev_per_user": total_fees / total_users if total_users else 0,
            "avg_rev_per_active_user": total_fees / active_users if active_users else 0,
        }

        # 3. Upsert it into cache DB
        upsert_avg_revenue_metrics(pd.DataFrame([result]))
        return result
    
def fetch_avg_revenue_metrics_for_range(start_date: date, days: int = 7) -> pd.DataFrame:
    """Compute avg revenue metrics for a single date range (used to update current week)."""
    from helpers.connection import get_main_db_connection, get_cache_db_connection
    with get_main_db_connection() as conn_main, get_cache_db_connection() as conn_cache:
        cur_main = conn_main.cursor()
        cur_cache = conn_cache.cursor()

        end_date = start_date + timedelta(days=days)

        # Total fees
        cur_cache.execute("SELECT SUM(value) FROM timeseries_fees WHERE date >= %s AND date < %s", (start_date, end_date))
        total_fees = cur_cache.fetchone()[0] or 0

        # Active users (SWAP txns)
        cur_main.execute('''
            SELECT COUNT(DISTINCT "userId")
            FROM "Activity"
            WHERE type = 'SWAP' AND status = 'SUCCESS'
              AND "createdAt" >= %s AND "createdAt" < %s
        ''', (start_date, end_date))
        active_users = cur_main.fetchone()[0] or 0

        row = {
            "week": start_date,
            "total_fees": total_fees,
            "active_users": active_users,
            "avg_rev_per_active_user": total_fees / active_users if active_users else 0
        }

        return pd.DataFrame([row])

def fetch_weekly_avg_revenue_metrics() -> pd.DataFrame:
    """Fetch weekly avg revenue per active user from cache DB."""
    with get_cache_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT week, total_fees, active_users, avg_rev_per_active_user
                FROM weekly_avg_revenue_metrics
                ORDER BY week
            """)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=[
                "week", "total_fees", "active_users", "avg_rev_per_active_user"
            ])
            df["week"] = pd.to_datetime(df["week"])  # ✅ Ensures .dt accessor will work
            return df