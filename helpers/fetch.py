import pandas as pd
from decimal import Decimal
import json
import streamlit as st
import requests
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict
from psycopg2.extras import RealDictCursor
from helpers.connection import get_cache_db_connection, get_main_db_connection
from helpers.constants import CHAIN_ID_MAP
from helpers.upsert import upsert_avg_revenue_metrics
from utils.transactions import normalize, parse_txn_json
from helpers.sync_utils import get_last_sync

HEADERS = {
    "Authorization": f"Basic {st.secrets['api']['AUTH_KEY']}"
}
ENDPOINTS = dict(st.secrets["api"])

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

def fetch_api_metric(key, start=None, end=None, username=None):
    url = ENDPOINTS[key]

    # Inject username into the URL path if applicable
    if username and "{username}" not in url:
        if url.endswith("/"):
            url += username
        else:
            url += f"/{username}"

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

        # Handle dict response like {"volume": 208.19, "qty": 13}
        if isinstance(data, dict):
            # Special case for full user profile with nested keys
            if key == "user_full_metrics":
                return data
            return pd.DataFrame([{"date": start, **data}])

        # Handle array-style responses
        elif isinstance(data, list):
            return pd.DataFrame(data)

        # Catch-all fallback
        return pd.DataFrame([{"date": start, "value": float(data)}])

    except Exception as e:
        print(f"[ERROR] fetch_api_metric failed: {e}")
        return pd.DataFrame() 

def fetch_avg_revenue_metrics(days: int = 30, snapshot_date: date = None) -> dict:
    snapshot_date = snapshot_date or date.today()

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

        # ✅ FIXED: Now uses cur_cache instead of cur_main
        cur_cache.execute('''
            SELECT COUNT(DISTINCT from_user)
            FROM transactions_cache
            WHERE type = 'SWAP' AND status = 'SUCCESS' AND created_at >= %s
        ''', (start_date,))
        active_users = cur_cache.fetchone()[0] or 0

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
        cur_cache.execute("""
            SELECT SUM(value)
            FROM timeseries_fees
            WHERE date >= %s AND date < %s
        """, (start_date, end_date))
        total_fees = cur_cache.fetchone()[0] or 0

        # Active users (SWAP txns)
        cur_cache.execute("""
            SELECT COUNT(DISTINCT from_user)
            FROM transactions_cache
            WHERE type = 'SWAP' AND status = 'SUCCESS'
              AND created_at >= %s AND created_at < %s
        """, (start_date, end_date))
        active_users = cur_cache.fetchone()[0] or 0

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
        
# === Utility Helpers ===
   
def format_token(token):
    if not token:
        return "N/A"
    symbol = token.get("symbol", "N/A")
    chain = token.get("chainId") or token.get("chain", "N/A")
    return f"{symbol} ({chain})"

def fetch_home_stats(main_conn, cache_conn):
    now = datetime.now()
    day_ago = now - timedelta(days=1)

    results = {
        "24h": defaultdict(float),
        "lifetime": defaultdict(float),
    }

    # === Transactions Cache Queries (Swap Volume, Sends, Transactions, Active Users) ===
    with cache_conn.cursor() as cursor:
        for scope, time_filter in [("24h", day_ago), ("lifetime", None)]:
            cursor.execute(f"""
                SELECT type, from_user, raw_transaction
                FROM transactions_cache
                WHERE status = 'SUCCESS'
                {f'AND created_at >= %s' if time_filter else ''}
            """, (time_filter,) if time_filter else ())

            users = set()
            for typ, from_user, txn_raw in cursor.fetchall():
                users.add(from_user)
                txn = parse_txn_json(txn_raw)
                from_amt = Decimal(txn.get("fromAmount", 0))
                from_token = txn.get("fromToken", {})
                price = Decimal(from_token.get("tokenPrices", {}).get("usd", 0))
                decimals = int(from_token.get("decimals", 18))

                usd_value = float(from_amt * price / Decimal(10**decimals)) if price > 0 else 0.0

                if typ == "SWAP":
                    results[scope]["swap_volume"] += usd_value
                    results[scope]["swaps"] += 1
                elif typ == "SEND":
                    results[scope]["crypto_sends"] += 1
                elif typ == "CASH":
                    sub_status = txn.get("subStatus")
                    if sub_status == "SEND":
                        results[scope]["cash_sends"] += 1

                results[scope]["transactions"] += 1

            results[scope]["active_users"] = len(users)

        # === Revenue (from timeseries_fees table) ===
        cursor.execute("""
            SELECT SUM(value) FROM timeseries_fees
            WHERE date >= %s
        """, (day_ago.date(),))
        rev_24h = cursor.fetchone()[0] or 0
        results["24h"]["revenue"] = float(rev_24h)

        cursor.execute("SELECT SUM(value) FROM timeseries_fees")
        rev_lifetime = cursor.fetchone()[0] or 0
        results["lifetime"]["revenue"] = float(rev_lifetime)

    # === User Table Queries (New Users, New Active Users, Total Users) ===
    with main_conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM \"User\"")
        results["lifetime"]["total_users"] = cursor.fetchone()[0]

        cursor.execute("""
            SELECT "userId", "createdAt" FROM "User"
        """)
        all_users = cursor.fetchall()
        new_users = {uid for uid, created in all_users if created >= day_ago}

    # Back in cache DB: identify recent active users
    with cache_conn.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT from_user FROM transactions_cache
            WHERE status = 'SUCCESS' AND created_at >= %s
        """, (day_ago,))
        active_24h_users = {row[0] for row in cursor.fetchall()}

    results["24h"]["new_users"] = len(new_users)
    results["24h"]["new_active_users"] = len(active_24h_users.intersection(new_users))
    results["lifetime"]["new_users"] = len(all_users)
    results["lifetime"]["new_active_users"] = results["lifetime"]["active_users"]

    return results

# === Recent Transactions ===
def fetch_recent_transactions(conn, limit=10):
    data = []

    # Load userId → username map
    user_id_map = {}
    with conn.cursor() as cursor:
        cursor.execute('SELECT "userId", username FROM "User"')
        for user_id, username in cursor.fetchall():
            user_id_map[user_id] = username

    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT "createdAt", "userId", type, status, transaction
            FROM "Activity"
            WHERE type != 'DAPP'
            ORDER BY "createdAt" DESC
            LIMIT %s
        """, (limit,))

        for created_at, user_id, typ, status, txn_raw in cursor.fetchall():
            txn = parse_txn_json(txn_raw)
            from_token, to_token = {}, {}
            amount_usd = 0

            # === From User ===
            from_user = (
                user_id_map.get(user_id)
                or txn.get("fromUsername")
                or txn.get("fromAddress")
                or user_id
                or "N/A"
            )

            # === To User ===
            to_user = "N/A"
            if typ in ("SWAP", "BRIDGE"):
                to_user = from_user

            elif typ == "SEND":
                to_addr = txn.get("toAddress", "").lower()
                if to_addr:
                    with conn.cursor() as c2:
                        c2.execute("""
                            SELECT u."userId"
                            FROM "Wallet" w
                            JOIN "WalletAccount" wa ON w."walletAccountId" = wa."id"
                            JOIN "User" u ON wa."userId" = u."userId"
                            WHERE LOWER(w."address") = %s
                            LIMIT 1
                        """, (to_addr,))
                        row = c2.fetchone()
                        if row:
                            to_user_id = row[0]
                            to_user = user_id_map.get(to_user_id, to_user_id)
                        else:
                            to_user = to_addr

            elif typ == "CASH":
                substatus = txn.get("subStatus")
                if substatus == "CONVERT":
                    to_user = txn.get("type", "CASH_CONVERT")
                else:
                    to_user_id = txn.get("toUserId")
                    to_user = (
                        user_id_map.get(to_user_id)
                        or txn.get("toUsername")
                        or txn.get("toExternalUser")
                        or to_user_id
                        or "N/A"
                    )

            # === Format Token ===
            def format_token(tkn):
                if not tkn:
                    return "N/A"
                symbol = tkn.get("symbol", "N/A")
                chain_id = tkn.get("chainId")
                chain = CHAIN_ID_MAP.get(chain_id, chain_id)
                return f"{symbol} ({chain})"

            def normalize_amount_safe(amount, token):
                try:
                    price = float(token.get("tokenPrices", {}).get("usd", 1))
                    decimals = int(token.get("decimals", 18))
                    return float(Decimal(amount) * Decimal(price) / Decimal(10**decimals))
                except Exception:
                    return 0

            # === Token + Amount Logic ===
            if typ == "SWAP":
                from_token = txn.get("fromToken", {})
                to_token = txn.get("toToken", {})
                amount_usd = normalize_amount_safe(txn.get("fromAmount", 0), from_token)

            elif typ == "SEND":
                from_token = txn.get("token", {})
                to_token = from_token
                amount_usd = normalize_amount_safe(txn.get("amount", 0), from_token)

            elif typ == "CASH":
                from_token = txn.get("token", {})
                to_token = from_token
                amount_usd = float(txn.get("amount", 0) or 0)

            elif typ == "BRIDGE":
                from_token = txn.get("fromToken", {})
                to_token = txn.get("toToken", {})
                amount_usd = normalize_amount_safe(txn.get("fromAmount", 0), from_token)

            # === Final Row Append ===
            data.append({
                "From User": from_user,
                "To User": to_user,
                "Date": created_at.strftime("%Y-%m-%d"),
                "Time": created_at.strftime("%H:%M:%S"),
                "Type": typ,
                "Status": status,
                "From Token (Chain)": format_token(from_token),
                "To Token (Chain)": format_token(to_token),
                "Amount USD": round(amount_usd, 2),
            })

    return data


def fetch_top_users_last_7d(conn, limit=10):
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    user_totals = defaultdict(float)

    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT u.username, a.transaction
            FROM "Activity" a
            JOIN "User" u ON a."userId" = u."userId"
            WHERE a.type = 'SWAP' AND a.status = 'SUCCESS' AND a."createdAt" >= %s
        """, (week_ago,))

        for username, txn_raw in cursor.fetchall():
            txn = parse_txn_json(txn_raw)
            from_amt = Decimal(txn.get("fromAmount", 0))
            from_token = txn.get("fromToken", {})
            price = Decimal(from_token.get("tokenPrices", {}).get("usd", 0))
            decimals = int(from_token.get("decimals", 18))

            usd_value = normalize(from_amt, price, decimals)
            user_totals[username] += usd_value

    return sorted(user_totals.items(), key=lambda x: x[1], reverse=True)[:limit]

def fetch_transactions_filtered(
    tx_type=None,
    min_amount=None,
    from_chain=None,
    to_chain=None,
    from_token=None,
    to_token=None,
    search_user_or_email=None,
    since_date=None,
    limit=500,
):
    conn = get_cache_db_connection()
    with conn.cursor() as cur:
        query = '''
            SELECT created_at, type, status, from_user, to_user,
                   from_token, from_chain, to_token, to_chain,
                   amount_usd, tx_hash, tx_display
            FROM transactions_cache
            WHERE 1=1
        '''
        params = []

        if tx_type:
            query += ' AND type = %s'
            params.append(tx_type)

        if min_amount:
            query += ' AND amount_usd >= %s'
            params.append(min_amount)

        if from_chain:
            query += ' AND from_chain = %s'
            params.append(from_chain)

        if to_chain:
            query += ' AND to_chain = %s'
            params.append(to_chain)

        if from_token:
            query += ' AND from_token = %s'
            params.append(from_token)

        if to_token:
            query += ' AND to_token = %s'
            params.append(to_token)

        if search_user_or_email:
            search_str = f"%{search_user_or_email.lower()}%"
            query += ' AND (LOWER(from_user) LIKE %s OR LOWER(to_user) LIKE %s)'
            params.extend([search_str, search_str])

        if since_date:
            query += ' AND created_at >= %s'
            params.append(since_date)

        query += ' ORDER BY created_at DESC LIMIT %s'
        params.append(limit)

        cur.execute(query, params)
        rows = cur.fetchall()

    sanitized = []
    for row in rows:
        row = list(row)
        row[3] = sanitize_username(row[3])  # from_user
        row[4] = sanitize_username(row[4])  # to_user
        sanitized.append(row)

    return sanitized

def sanitize_username(username):
    """
    Ensures the value is treated as a user identifier or label, and filters out None or unknown values.
    """
    if not username or str(username).lower() in ("none", "null", ""):
        return "Unknown"
    
    # If the value is a subStatus label, keep as-is
    known_labels = [
        "DEPOSIT", "WITHDRAW", "RECEIVE", "SEND", "CONVERT",
        "CONVERT: CASH TO CRYPTO", "CONVERT: CRYPTO TO CASH"
    ]
    if str(username).upper() in known_labels:
        return str(username).upper()

    return str(username)

def fetch_user_profile_summary(conn, query_input):
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # Try match on username or email
        cursor.execute("""
            SELECT u."userId", u.username, u.email, u."createdAt",
                   w.address AS wallet
            FROM "User" u
            LEFT JOIN "WalletAccount" wa ON wa."userId" = u."userId"
            LEFT JOIN "Wallet" w ON w."walletAccountId" = wa."id"
            WHERE LOWER(u.username) = LOWER(%s) OR LOWER(u.email) = LOWER(%s)
            LIMIT 1
        """, (query_input, query_input))
        result = cursor.fetchone()

        if result:
            return result

        # Try resolving wallet address → userId
        cursor.execute("""
            SELECT u."userId", u.username, u.email, u."createdAt", w.address
            FROM "Wallet" w
            JOIN "WalletAccount" wa ON w."walletAccountId" = wa."id"
            JOIN "User" u ON wa."userId" = u."userId"
            WHERE LOWER(w."address") = LOWER(%s)
            LIMIT 1
        """, (query_input,))
        return cursor.fetchone()
   
def fetch_user_metrics_full(username: str, start: str = None, end: str = None):
    result = {
        "profile": {},
        "cash": {},
        "crypto": {},
        "lifetime": {},
        "filtered": {}
    }

    try:
        # === Base Profile and Wallets (still fetched via API) ===
        full_profile = fetch_api_metric("user_full_metrics", username=username)
        if isinstance(full_profile, dict):
            result["cash"] = full_profile.get("cash", {})
            result["crypto"] = full_profile.get("crypto", {})
            result["profile"] = {
                "email": full_profile.get("email"),
                "createdAt": full_profile.get("createdAt"),
                "evm": full_profile.get("evmAddress"),
                "solana": full_profile.get("solaAddress"),
                "btc": full_profile.get("btcAddress"),
                "sui": full_profile.get("suiAddress"),
            }

        from_user = username  # Assuming this is the same label used in `transactions_cache`

        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                # === Lifetime SWAP volume and quantity
                cursor.execute("""
                    SELECT COALESCE(SUM(amount_usd), 0), COUNT(*)
                    FROM transactions_cache
                    WHERE type = 'SWAP' AND status = 'SUCCESS' AND from_user = %s
                """, (from_user,))
                volume, qty = cursor.fetchone()
                result["lifetime"]["volume"] = {
                    "volume": float(volume or 0),
                    "qty": int(qty or 0),
                }

                # === Lifetime referrals (optional: still fetched via API unless alternative source)
                referrals_data = fetch_api_metric("referrals", username=username)
                if isinstance(referrals_data, pd.DataFrame) and not referrals_data.empty:
                    row = referrals_data.iloc[0]
                    result["lifetime"]["referrals"] = int(row.get("value", 0))

                # === Filtered date-range volume and quantity
                if start:
                    if not end:
                        end = date.today().strftime("%Y-%m-%d")

                    cursor.execute("""
                        SELECT COALESCE(SUM(amount_usd), 0), COUNT(*)
                        FROM transactions_cache
                        WHERE type = 'SWAP' AND status = 'SUCCESS'
                          AND from_user = %s
                          AND created_at >= %s AND created_at < %s
                    """, (from_user, start, end))
                    volume, qty = cursor.fetchone()
                    result["filtered"]["volume"] = {
                        "volume": float(volume or 0),
                        "qty": int(qty or 0),
                    }

                    ref_filtered = fetch_api_metric("referrals", username=username, start=start, end=end)
                    if isinstance(ref_filtered, pd.DataFrame) and not ref_filtered.empty:
                        row = ref_filtered.iloc[0]
                        result["filtered"]["referrals"] = int(row.get("value", 0))

    except Exception as e:
        print(f"[ERROR] fetch_user_metrics_full failed: {e}")

    return result




