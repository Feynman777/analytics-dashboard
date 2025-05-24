from datetime import datetime, timedelta
import pandas as pd
from psycopg2.extras import execute_values
from helpers.api_utils import fetch_api_metric
from helpers.fetch.user import fetch_all_users

def upsert_daily_stats(start: datetime, end: datetime = None, conn=None):
    start_date = start.date()
    end_date = (end or datetime.now()).date() + timedelta(days=1)

    # === Load user data for new users and new active users ===
    all_users_df = fetch_all_users()
    if "created_at" in all_users_df.columns:
        all_users_df["created_at"] = pd.to_datetime(all_users_df["created_at"], errors="coerce").dt.date
    user_creation_map = dict(zip(all_users_df["user_id"], all_users_df["created_at"]))

    # === Load raw transactions ===
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DATE(created_at), from_chain, type, amount_usd, fee_usd, from_user
            FROM transactions_cache
            WHERE created_at >= %s AND created_at < %s AND status = 'SUCCESS'
        """, (start_date, end_date))
        rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=[
        "date", "chain_name", "type", "amount_usd", "fee_usd", "from_user"
    ])

    if df.empty:
        print("✅ No transaction data to process for daily_stats.")
        return

    stats = []
    grouped = df.groupby(["date", "chain_name"])

    for (date, chain), group in grouped:
        users_today = set(group["from_user"].dropna())
        new_users = {uid for uid in users_today if user_creation_map.get(uid) == date}
        new_active_users = {uid for uid in users_today if user_creation_map.get(uid) and user_creation_map[uid] < date}

        row = {
            "date": date,
            "chain_name": chain,
            "swap_transactions": (group["type"] == "SWAP").sum(),
            "swap_volume": group.loc[group["type"] == "SWAP", "amount_usd"].sum(),
            "swap_revenue": group.loc[group["type"] == "SWAP", "fee_usd"].sum(),
            "send_transactions": (group["type"] == "SEND").sum(),
            "send_volume": group.loc[group["type"] == "SEND", "amount_usd"].sum(),
            "cash_transactions": (group["type"] == "CASH").sum(),
            "cash_volume": group.loc[group["type"] == "CASH", "amount_usd"].sum(),
            "cash_revenue": group.loc[group["type"] == "CASH", "fee_usd"].sum(),
            "dapp_connections": (group["type"] == "DAPP").sum(),
            "referrals": 0,
            "agents_deployed": 0,
            "active_users": len(users_today),
            "new_users": len(new_users),
            "new_active_users": len(new_active_users),
            "revenue": group["fee_usd"].sum(),
        }

        for field, endpoint in {
            "referrals": "user/referrals",
            "agents_deployed": "agents/deployed",
        }.items():
            try:
                api_df = fetch_api_metric(endpoint, start=date.isoformat(), end=date.isoformat())
                if not api_df.empty:
                    row[field] = int(api_df.iloc[0].get("value", 0))
            except Exception:
                pass

        stats.append(row)

    if not stats:
        print("✅ No daily stats to upsert.")
        return

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO daily_stats (
                date, chain_name,
                swap_transactions, swap_volume, swap_revenue,
                send_transactions, send_volume,
                cash_transactions, cash_volume, cash_revenue,
                dapp_connections, referrals, agents_deployed,
                active_users, new_users, new_active_users,
                revenue
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
                new_users = EXCLUDED.new_users,
                new_active_users = EXCLUDED.new_active_users,
                revenue = EXCLUDED.revenue
        """, [
            (
                r["date"], r["chain_name"],
                int(r["swap_transactions"]), float(r["swap_volume"]), float(r["swap_revenue"]),
                int(r["send_transactions"]), float(r["send_volume"]),
                int(r["cash_transactions"]), float(r["cash_volume"]), float(r["cash_revenue"]),
                int(r["dapp_connections"]), int(r["referrals"]), int(r["agents_deployed"]),
                int(r["active_users"]), int(r["new_users"]), int(r["new_active_users"]),
                float(r["revenue"])
            ) for r in stats
        ])
        conn.commit()

    print(f"✅ Upserted {len(stats)} rows into daily_stats.")