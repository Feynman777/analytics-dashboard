# helpers\fetch\transactions.py

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import pandas as pd

from helpers.connection import get_cache_db_connection, get_main_db_connection
from helpers.utils.transactions import parse_txn_json, normalize, sanitize_username
from helpers.utils.constants import CHAIN_ID_MAP


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
    username=None,  # <- for backwards compatibility
) -> pd.DataFrame:
    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            query = '''
                SELECT created_at, type, status, from_user, to_user,
                       from_token, from_chain, to_token, to_chain,
                       amount_usd, tx_hash, tx_display
                FROM transactions_cache
                WHERE 1=1
            '''
            params = []

            if tx_type and tx_type != "All":
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

            # Allow both old `username` and new `search_user_or_email`
            if username:
                query += ' AND (LOWER(from_user) = %s OR LOWER(to_user) = %s)'
                username_lower = username.lower()
                params.extend([username_lower, username_lower])
            elif search_user_or_email:
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

    cols = [
        "created_at", "type", "status", "from_user", "to_user",
        "from_token", "from_chain", "to_token", "to_chain",
        "amount_usd", "tx_hash", "tx_display"
    ]

    df = pd.DataFrame(rows, columns=cols)
    df["from_user"] = df["from_user"].apply(sanitize_username)
    df["to_user"] = df["to_user"].apply(sanitize_username)
    return df


def fetch_recent_transactions(limit=10) -> list:
    data = []
    with get_main_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT "userId", username FROM "User"')
            user_id_map = dict(cursor.fetchall())

        with conn.cursor() as cursor:
            cursor.execute('''
                SELECT "createdAt", "userId", type, status, transaction
                FROM "Activity"
                WHERE type != 'DAPP'
                ORDER BY "createdAt" DESC
                LIMIT %s
            ''', (limit,))

            for created_at, user_id, typ, status, txn_raw in cursor.fetchall():
                txn = parse_txn_json(txn_raw)
                from_user = (
                    user_id_map.get(user_id)
                    or txn.get("fromUsername")
                    or txn.get("fromAddress")
                    or user_id
                    or "N/A"
                )

                to_user = from_user
                if typ == "SEND":
                    to_user = txn.get("toUsername") or txn.get("toAddress") or "N/A"
                elif typ == "CASH":
                    to_user = txn.get("toUserId") or txn.get("toUsername") or txn.get("toExternalUser") or "N/A"

                from_token = txn.get("fromToken", {}).get("symbol", "N/A")
                to_token = txn.get("toToken", {}).get("symbol", from_token)

                amount_usd = 0
                if typ in ["SWAP", "SEND", "BRIDGE"]:
                    amount = txn.get("fromAmount") or txn.get("amount")
                    token = txn.get("fromToken") or txn.get("token", {})
                    price = token.get("tokenPrices", {}).get("usd", 1)
                    decimals = token.get("decimals", 18)
                    amount_usd = normalize(amount, price, decimals)
                elif typ == "CASH":
                    amount_usd = float(txn.get("amount", 0))

                data.append({
                    "From User": from_user,
                    "To User": to_user,
                    "Date": created_at.strftime("%Y-%m-%d"),
                    "Time": created_at.strftime("%H:%M:%S"),
                    "Type": typ,
                    "Status": status,
                    "From Token (Chain)": from_token,
                    "To Token (Chain)": to_token,
                    "Amount USD": round(amount_usd, 2),
                })

    return data


def fetch_top_users_last_7d(conn=None, limit=10):
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    user_totals = defaultdict(float)

    conn = conn or get_main_db_connection()
    with conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                SELECT u.username, a.transaction
                FROM "Activity" a
                JOIN "User" u ON a."userId" = u."userId"
                WHERE a.type = 'SWAP' AND a.status = 'SUCCESS' AND a."createdAt" >= %s
            ''', (week_ago,))

            for username, txn_raw in cursor.fetchall():
                txn = parse_txn_json(txn_raw)
                from_amt = txn.get("fromAmount", 0)
                from_token = txn.get("fromToken", {})
                price = from_token.get("tokenPrices", {}).get("usd", 0)
                decimals = int(from_token.get("decimals", 18))
                usd_value = normalize(from_amt, price, decimals)
                user_totals[username] += usd_value

    return sorted(user_totals.items(), key=lambda x: x[1], reverse=True)[:limit]
