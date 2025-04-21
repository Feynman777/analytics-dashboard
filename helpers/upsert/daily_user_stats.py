from collections import defaultdict
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

from helpers.connection import get_main_db_connection

def upsert_daily_user_stats(start: datetime, conn):
    start_date = start.date()
    end_date = datetime.utcnow().date() + timedelta(days=1)

    with get_main_db_connection() as main_conn:
        with main_conn.cursor() as cur:
            cur.execute('SELECT "userId", username, "createdAt" FROM "User"')
            all_users = cur.fetchall()

    user_created_map = {
        user_id: created.date()
        for user_id, username, created in all_users if username
    }
    username_to_userId = {
        username.lower(): user_id
        for user_id, username, _ in all_users if username
    }

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT from_user
            FROM transactions_cache
            WHERE status = 'SUCCESS'
              AND type IN ('SWAP', 'SEND', 'CASH')
              AND created_at < %s
        """, (start_date,))
        past_active_usernames = {r[0].lower() for r in cur.fetchall() if r[0]}
        past_active_userIds = {
            username_to_userId[u] for u in past_active_usernames if u in username_to_userId
        }

    with conn.cursor() as cur:
        cur.execute("""
            SELECT DATE(created_at) AS date, type, from_user
            FROM transactions_cache
            WHERE status = 'SUCCESS'
              AND type IN ('SWAP', 'SEND', 'CASH')
              AND created_at >= %s AND created_at < %s
        """, (start_date, end_date))
        rows = cur.fetchall()

    daily_users = defaultdict(lambda: {"swap": set(), "send": set(), "cash": set()})

    for txn_date, typ, from_user in rows:
        if not from_user:
            continue
        user_id = username_to_userId.get(from_user.lower())
        if not user_id:
            continue
        daily_users[txn_date][typ.lower()].add(user_id)

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