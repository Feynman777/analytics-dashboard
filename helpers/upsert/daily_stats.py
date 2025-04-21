import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from helpers.api_utils import fetch_api_metric

def upsert_daily_stats(start: datetime, end: datetime = None, conn=None):
    start_date = start.date()
    end_date = (end or datetime.now()).date() + timedelta(days=1)

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
        print("✅ No data to process for daily_stats.")
        return

    grouped = df.groupby(["date", "chain_name"])
    stats = []

    for (date, chain), group in grouped:
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
            if r["type"] == "SWAP":
                row["swap_transactions"] += 1
                row["swap_volume"] += float(r["amount_usd"] or 0)
                row["swap_revenue"] += float(r["fee_usd"] or 0)
            elif r["type"] == "SEND":
                row["send_transactions"] += 1
                row["send_volume"] += float(r["amount_usd"] or 0)
            elif r["type"] == "CASH":
                row["cash_transactions"] += 1
                row["cash_volume"] += float(r["amount_usd"] or 0)
                row["cash_revenue"] += float(r["fee_usd"] or 0)
            elif r["type"] == "DAPP":
                row["dapp_connections"] += 1

        stats.append(row)

    # === Add referrals and agent deployments from API ===
    for row in stats:
        day_str = row["date"].isoformat()
        try:
            refs = fetch_api_metric("user/referrals", start=day_str, end=day_str)
            if not refs.empty:
                row["referrals"] = int(refs.iloc[0].get("value", 0))
        except Exception:
            pass
        try:
            agents = fetch_api_metric("agents/deployed", start=day_str, end=day_str)
            if not agents.empty:
                row["agents_deployed"] = int(agents.iloc[0].get("value", 0))
        except Exception:
            pass

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
