# helpers/sync/daily_user.py

from datetime import datetime, timedelta
from collections import defaultdict
from psycopg2.extras import execute_values

from helpers.fetch.users import fetch_all_users


def upsert_daily_user_stats(start: datetime, conn):
    start_date = start.date()
    end_date = datetime.now().date() + timedelta(days=1)

    # === Step 1: Load all users
    all_users = fetch_all_users()
    user_created_map = {u["user_id"]: u["created_at"].date() for u in all_users if u["username"]}
    username_to_userId = {u["username"].lower(): u["user_id"] for u in all_users if u["username"]}

    # === Step 2: Load past active users
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

    # === Step 3: Load transactions in range
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DATE(created_at) AS date, type, from_user
            FROM transactions_cache
            WHERE status = 'SUCCESS'
              AND type IN ('SWAP', 'SEND', 'CASH')
              AND created_at >= %s AND created_at < %s
        """, (start_date, end_date))
        rows = cur.fetchall()

    # === Step 4: Aggregate user activity by date and type
    daily_users = defaultdict(lambda: {"swap": set(), "send": set(), "cash": set()})
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

    # === Step 5: Compute and upsert
    rows_to_upsert = []
    for day in sorted(daily_users.keys()):
        swap_users = daily_users[day]["swap"]
        send_users = daily_users[day]["send"]
        cash_users = daily_users[day]["cash"]
        active_user_ids = swap_users | send_users | cash_users
        new_active_users = active_user_ids - past_active_userIds
        past_active_userIds.update(active_user_ids)
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
