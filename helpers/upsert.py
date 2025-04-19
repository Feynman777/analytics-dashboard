# upsert.py
import psycopg2
from collections import defaultdict
from psycopg2.extras import execute_values
import time
import pandas as pd
import hashlib
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import json
from helpers.connection import get_main_db_connection, get_cache_db_connection
from helpers.constants import CHAIN_ID_MAP
from utils.transactions import transform_activity_transaction, generate_fallback_tx_hash
from utils.transactions import safe_float, generate_fallback_tx_hash
from helpers.api_utils import fetch_api_metric
from helpers.fetch import fetch_home_stats

def upsert_daily_user_stats(start: datetime, conn):
    start_date = start.date()
    end_date = datetime.now().date() + timedelta(days=1)

    # Step 1: Load all users (userId + username + createdAt)
    with get_main_db_connection() as main_conn:
        with main_conn.cursor() as cur:
            cur.execute('SELECT "userId", username, "createdAt" FROM "User"')
            all_users = cur.fetchall()

    user_created_map = {}
    username_to_userId = {}

    for user_id, username, created in all_users:
        if not username:
            continue
        user_created_map[user_id] = created.date()
        username_to_userId[username.lower()] = user_id

    # Step 2: Load all past active userIds before the current window
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT from_user
            FROM transactions_cache
            WHERE status = 'SUCCESS'
              AND type IN ('SWAP', 'SEND', 'CASH')
              AND created_at < %s
        """, (start_date,))
        past_active_usernames = {r[0].lower() for r in cur.fetchall()}
        past_active_userIds = {
            username_to_userId[u] for u in past_active_usernames if u in username_to_userId
        }

    # Step 3: Load transactions within date range and map to userId
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DATE(created_at) AS date, type, from_user
            FROM transactions_cache
            WHERE status = 'SUCCESS'
              AND type IN ('SWAP', 'SEND', 'CASH')
              AND created_at >= %s
              AND created_at < %s
        """, (start_date, end_date))

        rows = cur.fetchall()

    # Step 4: Build per-day user activity by resolved userId
    daily_users = defaultdict(lambda: {
        "swap": set(),
        "send": set(),
        "cash": set()
    })

    for txn_date, typ, from_user in rows:
        if not from_user:
            continue
        user_id = username_to_userId.get(from_user.lower())
        if not user_id:
            continue

        if typ == "SWAP":
            daily_users[txn_date]["swap"].add(user_id)
        elif typ == "SEND":
            daily_users[txn_date]["send"].add(user_id)
        elif typ == "CASH":
            daily_users[txn_date]["cash"].add(user_id)

    # Step 5: Calculate active & new_active metrics
    rows_to_upsert = []

    for day in sorted(daily_users.keys()):
        swap_users = daily_users[day]["swap"]
        send_users = daily_users[day]["send"]
        cash_users = daily_users[day]["cash"]

        active_user_ids = swap_users | send_users | cash_users

        new_active_users = active_user_ids - past_active_userIds
        past_active_userIds.update(active_user_ids)  # Update running total

        new_users = {uid for uid, created in user_created_map.items() if created == day}

        rows_to_upsert.append((
            day,
            len(swap_users),
            len(send_users),
            len(cash_users),
            len(active_user_ids),
            len(new_users),
            len(new_active_users)
        ))

    if not rows_to_upsert:
        print("✅ No daily user stats to upsert.")
        return

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO daily_user_stats (
                date, active_swap, active_send, active_cash,
                total_active, new_users, new_active_users
            ) VALUES %s
            ON CONFLICT (date) DO UPDATE SET
                active_swap = EXCLUDED.active_swap,
                active_send = EXCLUDED.active_send,
                active_cash = EXCLUDED.active_cash,
                total_active = EXCLUDED.total_active,
                new_users = EXCLUDED.new_users,
                new_active_users = EXCLUDED.new_active_users
        """, rows_to_upsert)

    conn.commit()
    print(f"✅ Upserted {len(rows_to_upsert)} rows into daily_user_stats.")

def upsert_daily_stats(start: datetime, end: datetime = None, conn=None):
    """Aggregate daily stats from transactions_cache and upsert into daily_stats table."""
    start_date = start.date()
    end_date = (end or datetime.now(timezone.utc)).date() + timedelta(days=1)  # includes today

    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                DATE(created_at) AS date,
                from_chain AS chain_name,
                type,
                amount_usd,
                fee_usd,
                from_user
            FROM transactions_cache
            WHERE created_at >= %s AND created_at < %s AND status = 'SUCCESS'
        """, (start_date, end_date))
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=[
        "date", "chain_name", "type", "amount_usd", "fee_usd", "from_user"
    ])

    if df.empty:
        print("✅ No data to process for daily_stats.")
        return

    grouped = df.groupby(["date", "chain_name"])
    stats = []

    for (date, chain), group in grouped:
        print(f"📅 Processing {date} — Chain: {chain} — {len(group)} rows")
        row = {
            "date": date,
            "chain_name": chain,
            "swap_transactions": 0,
            "swap_volume": 0,
            "swap_revenue": 0,
            "send_transactions": 0,
            "send_volume": 0,
            "cash_transactions": 0,
            "cash_volume": 0,
            "cash_revenue": 0,
            "dapp_connections": 0,
            "referrals": 0,
            "agents_deployed": 0,
            "active_users": group["from_user"].nunique(),
            "revenue": group["fee_usd"].sum(),
        }

        for _, r in group.iterrows():
            typ = r["type"]
            amt = float(r["amount_usd"] or 0)
            fee = float(r["fee_usd"] or 0)

            if typ == "SWAP":
                row["swap_transactions"] += 1
                row["swap_volume"] += amt
                row["swap_revenue"] += fee
            elif typ == "SEND":
                row["send_transactions"] += 1
                row["send_volume"] += amt
            elif typ == "CASH":
                row["cash_transactions"] += 1
                row["cash_volume"] += amt
                row["cash_revenue"] += fee
            elif typ == "DAPP":
                row["dapp_connections"] += 1

        stats.append(row)

    # Step 2: Add referral + agent data via API (by day)
    for row in stats:
        day_str = row["date"].isoformat()
        try:
            referrals_df = fetch_api_metric("user/referrals", start=day_str, end=day_str)
            if isinstance(referrals_df, pd.DataFrame) and not referrals_df.empty:
                row["referrals"] = int(referrals_df.iloc[0].get("value", 0))
        except Exception as e:
            print(f"[WARN] Failed to fetch referrals for {day_str}: {e}")

        try:
            agents_df = fetch_api_metric("agents/deployed", start=day_str, end=day_str)
            if isinstance(agents_df, pd.DataFrame) and not agents_df.empty:
                row["agents_deployed"] = int(agents_df.iloc[0].get("value", 0))
        except Exception as e:
            print(f"[WARN] Failed to fetch agents for {day_str}: {e}")

    # Step 3: Upsert into daily_stats table
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO daily_stats (
                date, chain_name,
                swap_transactions, swap_volume, swap_revenue,
                send_transactions, send_volume,
                cash_transactions, cash_volume, cash_revenue,
                dapp_connections, referrals, agents_deployed,
                active_users, revenue
            ) VALUES %s
            ON CONFLICT (date, chain_name) DO UPDATE SET
                swap_transactions = EXCLUDED.swap_transactions,
                swap_volume = EXCLUDED.swap_volume,
                swap_revenue = EXCLUDED.swap_revenue,
                send_transactions = EXCLUDED.send_transactions,
                send_volume = EXCLUDED.send_volume,
                cash_transactions = EXCLUDED.cash_transactions,
                cash_volume = EXCLUDED.cash_volume,
                cash_revenue = EXCLUDED.cash_revenue,
                dapp_connections = EXCLUDED.dapp_connections,
                referrals = EXCLUDED.referrals,
                agents_deployed = EXCLUDED.agents_deployed,
                active_users = EXCLUDED.active_users,
                revenue = EXCLUDED.revenue
        """, [(
            r["date"], r["chain_name"],
            r["swap_transactions"], r["swap_volume"], r["swap_revenue"],
            r["send_transactions"], r["send_volume"],
            r["cash_transactions"], r["cash_volume"], r["cash_revenue"],
            r["dapp_connections"], r["referrals"], r["agents_deployed"],
            r["active_users"], r["revenue"]
        ) for r in stats])

    conn.commit()
    print(f"✅ Upserted {len(stats)} rows into daily_stats.")



def get_latest_cached_timestamp():
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(created_at) FROM transactions_cache")
                return cur.fetchone()[0] or datetime(2024, 1, 1, tzinfo=timezone.utc)
    except Exception as e:
        print(f"❌ Error fetching latest created_at from cache: {e}")
        return datetime(2024, 1, 1, tzinfo=timezone.utc)
    
def upsert_chain_timeseries(df):
    """
    Upserts a list of dictionaries into the timeseries_chain_volume table.

    Each dictionary should contain the following keys:
    - date (str or datetime.date)
    - chain (str)
    - metric (str)
    - status (str)
    - value (float)
    - quantity (int)
    """
    if df.empty:
        return

    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO timeseries_chain_volume (date, chain, metric, status, value, quantity)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, chain, metric, status)
                        DO UPDATE SET value = EXCLUDED.value, quantity = EXCLUDED.quantity
                    """, (
                        row['date'],
                        row['chain'],
                        row['metric'],
                        row['status'],
                        row['value'],
                        row['quantity']
                    ))
                conn.commit()
    except Exception as e:
        print(f"Error in upsert_chain_timeseries: {e}")
        raise

def upsert_timeseries(metric, df):
    """
    Upserts a pandas DataFrame into the timeseries_cache table.

    Parameters:
    - metric (str): the name of the metric
    - df (pd.DataFrame): must contain columns ['date', 'value']
    """
    if df.empty:
        return

    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                batch_size = 50
                for start in range(0, len(df), batch_size):
                    batch = df.iloc[start:start + batch_size]
                    for attempt in range(3):
                        try:
                            for _, row in batch.iterrows():
                                date = row['date'].strftime('%Y-%m-%d')
                                value = float(row['value'])
                                cursor.execute("""
                                    INSERT INTO timeseries_cache (metric, date, value)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (metric, date)
                                    DO UPDATE SET value = EXCLUDED.value
                                """, (metric, date, value))
                            conn.commit()
                            break
                        except psycopg2.errors.DeadlockDetected:
                            conn.rollback()
                            time.sleep(1)
                            if attempt == 2:
                                raise
                        except Exception as e:
                            conn.rollback()
                            raise
    except Exception as e:
        print(f"Error in upsert_timeseries: {e}")

def upsert_fee_series(df):
    """Insert or update fee data into timeseries_fees table."""
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO timeseries_fees (date, chain, value)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (date, chain)
                        DO UPDATE SET value = EXCLUDED.value
                    """, (row["date"], row["chain"], row["value"]))
                conn.commit()
    except Exception as e:
        print(f"[ERROR] Failed to upsert fees: {e}")
        raise

def upsert_avg_revenue_metrics(df):
    """Insert or update average revenue metrics into avg_revenue_metrics table."""
    from helpers.connection import get_cache_db_connection

    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO avg_revenue_metrics (
                            date, total_fees, total_users, active_users,
                            avg_rev_per_user, avg_rev_per_active_user
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date)
                        DO UPDATE SET
                            total_fees = EXCLUDED.total_fees,
                            total_users = EXCLUDED.total_users,
                            active_users = EXCLUDED.active_users,
                            avg_rev_per_user = EXCLUDED.avg_rev_per_user,
                            avg_rev_per_active_user = EXCLUDED.avg_rev_per_active_user
                    """, (
                        row["date"], row["total_fees"], row["total_users"],
                        row["active_users"], row["avg_rev_per_user"], row["avg_rev_per_active_user"]
                    ))
            conn.commit()
    except Exception as e:
        print(f"[ERROR] Failed to upsert avg revenue metrics: {e}")
        raise

def upsert_weekly_avg_revenue_metrics(df: pd.DataFrame):
    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO weekly_avg_revenue_metrics (week, total_fees, active_users, avg_rev_per_active_user)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (week) DO UPDATE
                    SET total_fees = EXCLUDED.total_fees,
                        active_users = EXCLUDED.active_users,
                        avg_rev_per_active_user = EXCLUDED.avg_rev_per_active_user
                """, (row["week"], row["total_fees"], row["active_users"], row["avg_rev_per_active_user"]))
        conn.commit()

def upsert_transactions_from_activity(force=False, batch_size=100, start=None, end=None):
    main_conn = get_main_db_connection()
    cache_conn = get_cache_db_connection()

    with main_conn.cursor() as cur_main, cache_conn.cursor() as cur_cache:
        # Determine sync range
        if start:
            sync_start = start
        elif force:
            sync_start = datetime.now(timezone.utc) - timedelta(hours=2)
        else:
            cur_cache.execute("SELECT MAX(created_at) FROM transactions_cache")
            latest_cached = cur_cache.fetchone()[0]
            sync_start = latest_cached - timedelta(hours=2) if latest_cached else datetime(2025, 4, 14, tzinfo=timezone.utc)

        sync_end = end or datetime.now(timezone.utc)

        cur_main.execute("""
            SELECT COUNT(*) FROM "Activity"
            WHERE "createdAt" >= %s AND "createdAt" < %s
        """, (sync_start, sync_end))
        total_rows = cur_main.fetchone()[0]

        insert_count = 0

        for offset in range(0, total_rows, batch_size):
            cur_main.execute("""
                SELECT "createdAt", "userId", type, status, hash, transaction, "chainIds"
                FROM "Activity"
                WHERE "createdAt" >= %s AND "createdAt" < %s
                ORDER BY "createdAt" ASC
                LIMIT %s OFFSET %s
            """, (sync_start, sync_end, batch_size, offset))

            rows = cur_main.fetchall()
            if not rows:
                break

            for created_at, user_id, typ, status, tx_hash, txn_raw, chain_ids in rows:
                try:
                    tx_data = transform_activity_transaction(
                        tx_hash=tx_hash,
                        txn_raw=txn_raw,
                        typ=typ,
                        status=status,
                        created_at=created_at,
                        user_id=user_id,
                        conn=main_conn,
                        chain_ids=chain_ids
                    )

                    if not tx_data or not tx_data.get("tx_hash"):
                        continue

                    # Lock in FAIL for fallback SWAPs
                    if tx_data["type"] == "SWAP" and tx_data["tx_hash"].startswith("unknown-"):
                        tx_data["status"] = "FAIL"

                    to_user = tx_data["to_user"]
                    if isinstance(to_user, dict):
                        to_user = to_user.get("username")

                    cur_cache.execute("""
                        INSERT INTO transactions_cache (
                            created_at, type, status, from_user, to_user,
                            from_token, from_chain, to_token, to_chain,
                            amount_usd, fee_usd, tx_hash, chain_id, tx_display
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tx_hash) DO UPDATE SET
                            amount_usd = EXCLUDED.amount_usd,
                            fee_usd = EXCLUDED.fee_usd,
                            to_user = EXCLUDED.to_user,
                            from_user = EXCLUDED.from_user,
                            from_token = EXCLUDED.from_token,
                            to_token = EXCLUDED.to_token,
                            from_chain = EXCLUDED.from_chain,
                            to_chain = EXCLUDED.to_chain,
                            status = EXCLUDED.status,
                            tx_display = EXCLUDED.tx_display,
                            created_at = EXCLUDED.created_at
                    """, (
                        tx_data["created_at"], tx_data["type"], tx_data["status"],
                        tx_data["from_user"], to_user,
                        tx_data["from_token"], tx_data["from_chain"],
                        tx_data["to_token"], tx_data["to_chain"],
                        safe_float(tx_data.get("amount_usd")), safe_float(tx_data.get("fee_usd")),
                        tx_data["tx_hash"], tx_data["chain_id"], tx_data.get("tx_display")
                    ))

                    insert_count += 1

                except Exception:
                    continue  # Fail silently in production unless you want to log exceptions

            cache_conn.commit()
