# upsert.py
import psycopg2
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

    print("🔥 upsert_transactions_from_activity() called")

    main_conn = get_main_db_connection()
    cache_conn = get_cache_db_connection()

    with main_conn.cursor() as cur_main, cache_conn.cursor() as cur_cache:
        # Determine sync range
        if start:
            sync_start = start
        elif force:
            sync_start = datetime.now(timezone.utc) - timedelta(hours=4)
        else:
            cur_cache.execute("SELECT MAX(created_at) FROM transactions_cache")
            latest_cached = cur_cache.fetchone()[0]
            if latest_cached:
                sync_start = latest_cached - timedelta(hours=12)
            else:
                sync_start = datetime(2025, 4, 14, tzinfo=timezone.utc)

        sync_end = end or datetime.now(timezone.utc)

        print(f"⏱️ Syncing from: {sync_start} → {sync_end} (force={force})")

        cur_main.execute('''
            SELECT COUNT(*) FROM "Activity"
            WHERE "createdAt" >= %s AND "createdAt" < %s
        ''', (sync_start, sync_end))
        total_rows = cur_main.fetchone()[0]
        print(f"🔍 Total rows to sync: {total_rows}")

        insert_count = 0

        for offset in range(0, total_rows, batch_size):
            print(f"\n⏳ Syncing batch: {offset} → {offset + batch_size}")
            cur_main.execute('''
                SELECT "createdAt", "userId", type, status, hash, transaction, "chainIds"
                FROM "Activity"
                WHERE "createdAt" >= %s AND "createdAt" < %s
                ORDER BY "createdAt" ASC
                LIMIT %s OFFSET %s
            ''', (sync_start, sync_end, batch_size, offset))

            rows = cur_main.fetchall()
            print(f"📦 Rows fetched: {len(rows)}")
            #for preview in rows[:3]:
                #print("[Preview Activity]", preview)

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

                    #tx_data["tx_hash"] = tx_data.get("tx_hash") or generate_fallback_tx_hash(created_at, txn_raw)
                    # ✅ Add this null guard immediately after transform
                    if not tx_data or not tx_data.get("tx_hash"):
                        print("❗ tx_hash is STILL None after transform — skipping this transaction")
                        continue

                    to_user = tx_data["to_user"]
                    if isinstance(to_user, dict):
                        to_user = to_user.get("username")


                    if tx_data["type"] == "SWAP" and tx_data["tx_hash"].startswith("unknown"):
                        print("🚨 About to write fallback SWAP:")
                        print(f"  status = {tx_data['status']}")
                        print(f"  tx_hash = {tx_data['tx_hash']}")
                        print(f"  full row = {tx_data}")

                    
                    if tx_data["type"] == "SWAP" and tx_data["tx_hash"] and tx_data["tx_hash"].startswith("unknown"):
                        tx_data["status"] = "FAIL"

                    print(f"📝 UPSERTING: tx_hash={tx_data['tx_hash']} | type={tx_data['type']} | status={tx_data['status']}")
                    if tx_data["tx_hash"].startswith("unknown-") and tx_data["type"] == "SWAP":
                        tx_data["status"] = "FAIL"  # lock in FAIL, always
                        print(f"🛡️ FINAL OVERRIDE: Force FAIL for unknown fallback SWAP: {tx_data['tx_hash']}")

                    cur_cache.execute('''
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
                    ''', (
                        tx_data["created_at"], tx_data["type"], tx_data["status"],
                        tx_data["from_user"], to_user,
                        tx_data["from_token"], tx_data["from_chain"],
                        tx_data["to_token"], tx_data["to_chain"],
                        safe_float(tx_data.get("amount_usd")), safe_float(tx_data.get("fee_usd")),
                        tx_data["tx_hash"], tx_data["chain_id"], tx_data.get("tx_display")
                    ))

                    insert_count += 1

                except Exception as e:
                    print("❌ Failed to process txn with values:")
                    print(f"  tx_hash: {tx_hash}")
                    print(f"  created_at: {created_at}")
                    print(f"  fee_usd: {tx_data.get('fee_usd') if 'tx_data' in locals() else 'N/A'}")
                    print(f"  amount_usd: {tx_data.get('amount_usd') if 'tx_data' in locals() else 'N/A'}")
                    import traceback
                    print(traceback.format_exc())
                    continue

            cache_conn.commit()
            print(f"✅ Batch {offset // batch_size + 1} committed — {insert_count} transactions upserted.")

    print("🎉 Upsert from Activity complete.")