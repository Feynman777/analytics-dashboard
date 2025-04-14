# upsert.py
import psycopg2
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
from tqdm import tqdm
import json
from helpers.connection import get_main_db_connection, get_cache_db_connection
from helpers.constants import CHAIN_ID_MAP
from utils.transactions import transform_activity_transaction, resolve_username_by_userid, resolve_username_by_address

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
    from helpers.connection import get_cache_db_connection
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

def generate_fallback_tx_hash(created_at, offset):
    timestamp_str = created_at.strftime("%Y%m%d%H%M%S")
    return f"unknown-{timestamp_str}-{offset}"

def upsert_transactions_from_activity(force=False, batch_size=100):
    print("🔥 upsert_transactions_from_activity() called")

    main_conn = get_main_db_connection()
    cache_conn = get_cache_db_connection()

    with main_conn.cursor() as cur_main, cache_conn.cursor() as cur_cache:
        cur_cache.execute("SELECT MAX(created_at) FROM transactions_cache")
        latest_cached = cur_cache.fetchone()[0]
        if latest_cached:
            sync_start = latest_cached - timedelta(hours=4)
        else:
            sync_start = datetime(2024, 3, 19)

        cur_main.execute('''
            SELECT COUNT(*) FROM "Activity"
            WHERE "createdAt" >= %s
        ''', (sync_start,))
        total_rows = cur_main.fetchone()[0]
        print(f"Total rows to sync: {total_rows}")

        for offset in range(0, total_rows, batch_size):
            print(f"\n⏳ Syncing batch: {offset} → {offset + batch_size}")
            cur_main.execute('''
                SELECT "createdAt", "userId", type, status, hash, transaction, "chainIds"
                FROM "Activity"
                WHERE "createdAt" >= %s
                ORDER BY "createdAt" ASC
                LIMIT %s OFFSET %s
            ''', (sync_start, batch_size, offset))

            rows = cur_main.fetchall()
            if not rows:
                break

            for idx, (created_at, user_id, typ, status, tx_hash, txn_raw, chain_ids) in enumerate(rows):
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

                    tx_data["tx_hash"] = tx_hash or generate_fallback_tx_hash(created_at, offset + idx)

                    cur_cache.execute('''
                        INSERT INTO transactions_cache (
                            created_at, type, status, from_user, to_user,
                            from_token, from_chain, to_token, to_chain,
                            amount_usd, tx_hash, chain_id, raw_transaction
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tx_hash) DO UPDATE SET
                            amount_usd = EXCLUDED.amount_usd,
                            to_user = EXCLUDED.to_user,
                            from_user = EXCLUDED.from_user,
                            from_token = EXCLUDED.from_token,
                            to_token = EXCLUDED.to_token,
                            from_chain = EXCLUDED.from_chain,
                            to_chain = EXCLUDED.to_chain,
                            raw_transaction = EXCLUDED.raw_transaction,
                            status = EXCLUDED.status,
                            created_at = EXCLUDED.created_at
                    ''', (
                        tx_data["created_at"], tx_data["type"], tx_data["status"],
                        tx_data["from_user"], tx_data["to_user"],
                        tx_data["from_token"], tx_data["from_chain"],
                        tx_data["to_token"], tx_data["to_chain"],
                        tx_data["amount_usd"], tx_data["tx_hash"],
                        tx_data["chain_id"], json.dumps(tx_data["raw_transaction"])
                    ))

                except Exception as e:
                    print(f"❌ Failed to process txn at {created_at}: {e}")
                    continue

            cache_conn.commit()
            print(f"✅ Batch {offset // batch_size + 1} committed.")

    print("🎉 Upsert from Activity complete.")