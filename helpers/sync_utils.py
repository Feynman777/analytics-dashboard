import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, timezone
import json
import os
from helpers.upsert import upsert_transactions_from_activity
from helpers.connection import get_cache_db_connection, get_main_db_connection
from helpers.upsert import upsert_fee_series, upsert_weekly_avg_revenue_metrics
from helpers.constants import CHAIN_ID_MAP
from utils.transactions import transform_activity_transaction
from helpers.fee_utils import fetch_fee_series
from utils.safe_math import safe_float

SYNC_FILE = "last_sync.json"

SECTION_DELTA_MAP = {
    "Transactions": timedelta(hours=1),
    "Financials": timedelta(hours=1),
    "Weekly_Data": timedelta(hours=1),
}

def sync_weekly_avg_revenue_metrics():
    from helpers.fetch import fetch_avg_revenue_metrics_for_range
    from helpers.upsert import upsert_weekly_avg_revenue_metrics

    print("📆 Syncing weekly average revenue metrics...")

    today = datetime.now(timezone.utc).date()
    this_monday = today - timedelta(days=today.weekday())  # start of current week
    prev_monday = this_monday - timedelta(days=7)          # start of previous week

    # Get data for previous and current weeks
    df = fetch_avg_revenue_metrics_for_range(prev_monday, days=7)
    upsert_weekly_avg_revenue_metrics(df)

    print(f"✅ Weekly average revenue metrics synced ({prev_monday} to {prev_monday + timedelta(days=7)})")

def sync_weekly_data():
    from helpers.fetch import fetch_swap_series, fetch_api_metric
    from helpers.upsert import upsert_chain_timeseries, upsert_timeseries
    from helpers.sync_utils import get_last_sync, update_last_sync

    SECTION_KEY = "Weekly_Data"
    now = datetime.now(timezone.utc)
    last_sync = get_last_sync(SECTION_KEY).replace(tzinfo=timezone.utc)
    start_date = (last_sync - timedelta(hours=2)).date()
    end_date = now.date()

    print(f"🔁 Running sync_weekly_data from {start_date} to {end_date}")

    # === Sync SWAP volume
    df_swaps = pd.DataFrame(fetch_swap_series(start=start_date, end=end_date))
    if not df_swaps.empty:
        df_swaps["date"] = pd.to_datetime(df_swaps["date"]).dt.date
        df_swaps["metric"] = "swap_volume"
        df_swaps["status"] = "success"
        df_swaps["quantity"] = df_swaps["quantity"].astype(int)
        upsert_chain_timeseries(df_swaps)

    # === Sync API metrics
    API_METRICS = ["cash_volume", "new_users", "referrals", "total_agents"]
    for metric in API_METRICS:
        rows = []
        for d in pd.date_range(start=start_date, end=end_date):
            df = fetch_api_metric(metric, d.strftime("%Y-%m-%d"))
            if not df.empty:
                df["date"] = pd.to_datetime(df["date"]).dt.date
                df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)
                rows.append(df)
        if rows:
            all_df = pd.concat(rows)
            upsert_timeseries(metric, all_df)

    update_last_sync(SECTION_KEY, now)
    print(f"✅ Weekly data synced and last_sync.json updated to {now}")

def sync_financials():
    from helpers.upsert import upsert_financials
    SECTION_KEY = "Financials"
    now = datetime.now(timezone.utc)
    start = get_last_sync(SECTION_KEY).replace(tzinfo=timezone.utc)

    print(f"🔁 Running sync_financials from {start} to {now}")

    try:
        with get_cache_db_connection() as conn:
            upsert_financials(start=start, end=now, conn=conn)
            update_last_sync(SECTION_KEY, now)
            print(f"✅ Financials synced and last_sync.json updated to {now}")
    except Exception as e:
        print(f"❌ Error syncing financials: {e}")


def sync_daily_stats():
    from helpers.upsert import upsert_daily_stats
    SECTION_KEY = "Daily_Stats"
    now = datetime.now(timezone.utc)

    # Get last sync and round down to midnight UTC
    last_sync = get_last_sync(SECTION_KEY).replace(tzinfo=timezone.utc)
    start = last_sync.replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"🔁 Running sync_daily_stats from {start}")

    try:
        with get_cache_db_connection() as conn:
            upsert_daily_stats(start=start, conn=conn)
            update_last_sync(SECTION_KEY, now)
            print(f"✅ Daily stats synced and last_sync.json updated to {now}")
    except Exception as e:
        print(f"❌ Error syncing daily stats: {e}")
        update_last_sync(SECTION_KEY, now)


def patch_sui_failures_as_success(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE transactions_cache
            SET status = 'SUCCESS'
            WHERE from_chain = 'sui'
              AND status = 'FAIL'
              AND tx_hash IS NOT NULL
              AND LENGTH(tx_hash) > 10
        """)
        print(f"[PATCH] Corrected {cursor.rowcount} misclassified SUI transactions")
        conn.commit()

def sync_transaction_cache(force=False):
    SECTION_KEY = "Transactions"
    BATCH_SIZE = 100

    now = datetime.now(timezone.utc)
    last_sync = get_last_sync(SECTION_KEY).replace(tzinfo=timezone.utc)
    main_start = last_sync
    main_end = now
    pending_start = now - timedelta(days=1)

    print(f"🔁 Full sync from {main_start.isoformat()} → {main_end.isoformat()}")
    print(f"🔁 Rechecking PENDING txns from {pending_start.isoformat()} → {main_end.isoformat()}")

    def fetch_rows(start, end, pending_only=False):
        filter_clause = "status = 'PENDING' AND" if pending_only else ""
        with get_main_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT "createdAt", "userId", type, status, hash, transaction, "chainIds"
                    FROM "Activity"
                    WHERE {filter_clause} "createdAt" >= %s AND "createdAt" < %s
                    AND type IN ('SWAP', 'SEND', 'CASH', 'DAPP')
                    ORDER BY "createdAt" ASC
                """, (start, end))
                return cur.fetchall(), conn

    main_rows, main_conn = fetch_rows(main_start, main_end)
    pending_rows, _ = fetch_rows(pending_start, main_end, pending_only=True)
    all_rows = main_rows + pending_rows

    print(f"📦 Total rows to sync (new + pending): {len(all_rows)}")

    with get_cache_db_connection() as cache_conn:
        with main_conn.cursor() as cur_main, cache_conn.cursor() as cur_cache:
            insert_count = 0

            for i, row in enumerate(all_rows):
                if i % BATCH_SIZE == 0:
                    print(f"⏳ Processing row {i + 1} / {len(all_rows)}")

                created_at, user_id, typ, status, tx_hash, txn_raw, chain_ids = row

                tx_data = transform_activity_transaction(
                    tx_hash=tx_hash,
                    txn_raw=txn_raw,
                    typ=typ,
                    status=status,
                    created_at=created_at,
                    user_id=user_id,
                    conn=main_conn,
                    chain_ids=chain_ids,
                )

                if not tx_data or not tx_data.get("tx_hash"):
                    continue

                to_user = tx_data["to_user"]
                if isinstance(to_user, dict):
                    to_user = to_user.get("username")

                if tx_data["tx_hash"].startswith("unknown-") and tx_data["type"] == "SWAP":
                    tx_data["status"] = "FAIL"

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

            cache_conn.commit()

            # Get latest created_at from transactions_cache
            cur_cache.execute("SELECT MAX(created_at) FROM transactions_cache")
            latest_ts = cur_cache.fetchone()[0] or now
            update_last_sync(SECTION_KEY, latest_ts)

    print(f"✅ Sync complete — {insert_count} rows inserted/updated. Last sync updated to {latest_ts.isoformat()}")



def get_last_sync(section: str) -> datetime:
    print(f"🕐 Fetching last sync for: {section}")
    try:
        if os.path.exists(SYNC_FILE):
            with open(SYNC_FILE, "r") as f:
                data = json.load(f)
                raw = data.get(section)
                if raw:
                    dt = datetime.fromisoformat(raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
    except Exception as e:
        print(f"❌ Error reading last_sync.json: {e}")
    return datetime(2024, 1, 1, tzinfo=timezone.utc)

def update_last_sync(section: str, sync_datetime: datetime):
    print(f"💾 Updating last sync for: {section} → {sync_datetime.isoformat()}")
    print(f"🔍 Sync file path: {os.path.abspath(SYNC_FILE)}")
    try:
        data = {}
        if os.path.exists(SYNC_FILE):
            with open(SYNC_FILE, "r") as f:
                data = json.load(f)
        data[section] = sync_datetime.isoformat()
        with open(SYNC_FILE, "w") as f:
            json.dump(data, f, indent=2)
        print("✅ Sync file updated successfully.")
    except Exception as e:
        print(f"❌ Error writing to last_sync.json: {e}")

def sync_section(section_name: str, sync_callback):
    now = datetime.now(timezone.utc)
    last_sync = get_last_sync(section_name)
    delta = SECTION_DELTA_MAP.get(section_name, timedelta(hours=4))
    force = False

    if force or (now - last_sync >= delta):
        with st.spinner(f"Syncing {section_name.replace('_', ' ')} from {last_sync.date()} to {now.date()}..."):
            try:
                sync_callback(last_sync, now)
                update_last_sync(section_name, now)
                st.success(f"✅ {section_name.replace('_', ' ')} synced successfully.")
            except Exception as e:
                st.error(f"❌ Sync failed: {e}")
    else:
        st.success(f"✅ Last synced at: `{last_sync.strftime('%Y-%m-%d %H:%M')} UTC`")


def sync_fee_series():
    from helpers.upsert import upsert_fee_series
    from helpers.fee_utils import fetch_fee_series  # Make sure you're importing it if it's not local
    SECTION_KEY = "Fee_Series"
    now = datetime.now(timezone.utc)

    start = get_last_sync(SECTION_KEY).replace(tzinfo=timezone.utc)
    print(f"🔁 Running sync_fee_series from {start.date()} to {now.date()}")

    try:
        df = fetch_fee_series(start=start)
        
        if df.empty or "date" not in df.columns:
            print("⚠️ No fee data found or missing 'date' column.")
            update_last_sync(SECTION_KEY, now)
            return

        print(f"📊 Found {len(df)} fee records to upsert (from {df['date'].min()} to {df['date'].max()})")
        for i in range(0, len(df), 100):
            print(f"  ⏳ Upserting batch {i} → {min(i+100, len(df))}")
            upsert_fee_series(df.iloc[i:i+100])

        update_last_sync(SECTION_KEY, now)
        print(f"✅ Fee series sync complete. Last sync updated to {now.isoformat()}")

    except Exception as e:
        print(f"❌ Error syncing fee series: {e}")

