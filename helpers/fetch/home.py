from collections import defaultdict
from datetime import datetime, timedelta, timezone
from psycopg2.extensions import connection
from helpers.fetch.cash_yield import fetch_cash_yield_metrics


def fetch_home_stats(main_conn: connection, cache_conn: connection) -> dict:
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=24)

    results = {
        "24h": defaultdict(float),
        "lifetime": defaultdict(float),
    }

    def fetch_transactions(scope: str, since: datetime = None):
        query = """
            SELECT type, from_user, amount_usd, fee_usd
            FROM transactions_cache
            WHERE status = 'SUCCESS'
        """
        params = []
        if since:
            query += " AND created_at >= %s"
            params.append(since)

        with cache_conn.cursor() as cursor:
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

            user_set = set()
            for typ, from_user, amount_usd, fee_usd in rows:
                user_set.add(from_user)
                amount = float(amount_usd or 0)
                fee = float(fee_usd or 0)

                if typ == "SWAP":
                    results[scope]["swap_volume"] += amount
                    results[scope]["swap_transactions"] += 1
                    results[scope]["swap_revenue"] += fee
                elif typ == "SEND":
                    results[scope]["send_transactions"] += 1
                    results[scope]["send_volume"] += amount
                elif typ == "CASH":
                    results[scope]["cash_transactions"] += 1
                    results[scope]["cash_volume"] += amount
                    results[scope]["cash_revenue"] += fee

                results[scope]["transactions"] += 1

            results[scope]["active_users"] = len(user_set)

    # === Fetch transaction aggregates
    fetch_transactions("24h", window_start)
    fetch_transactions("lifetime")

    # === Revenue fallback
    with cache_conn.cursor() as cursor:
        cursor.execute("""
            SELECT SUM(fee_usd) FROM transactions_cache
            WHERE status = 'SUCCESS' AND created_at >= %s
        """, (window_start,))
        results["24h"]["revenue"] = float(cursor.fetchone()[0] or 0)

        cursor.execute("""
            SELECT SUM(fee_usd) FROM transactions_cache
            WHERE status = 'SUCCESS'
        """)
        results["lifetime"]["revenue"] = float(cursor.fetchone()[0] or 0)

    # === Lifetime cash stats from daily_stats
    with cache_conn.cursor() as cursor:
        cursor.execute("""
            SELECT SUM(cash_transactions), SUM(cash_volume)
            FROM daily_stats
        """)
        row = cursor.fetchone()
        results["lifetime"]["cash_transactions"] = row[0] or 0
        results["lifetime"]["cash_volume"] = float(row[1] or 0)

    # === User counts from main DB
    with main_conn.cursor() as cursor:
        cursor.execute('SELECT COUNT(*) FROM "User"')
        results["lifetime"]["total_users"] = cursor.fetchone()[0]

        cursor.execute('SELECT "userId", "createdAt" FROM "User"')
        all_users = cursor.fetchall()
        new_users = {
            uid for uid, created in all_users
            if created.replace(tzinfo=timezone.utc) >= window_start
        }

    # === Active 24h users (from cache)
    with cache_conn.cursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT from_user FROM transactions_cache
            WHERE status = 'SUCCESS' AND created_at >= %s
        """, (window_start,))
        active_24h_users = {row[0] for row in cursor.fetchall()}

    results["24h"]["new_users"] = len(new_users)
    results["24h"]["new_active_users"] = len(active_24h_users.intersection(new_users))
    results["lifetime"]["new_users"] = len(all_users)
    results["lifetime"]["new_active_users"] = results["lifetime"]["active_users"]

    # === Cash yield via API
    try:
        lifetime_yield, yield_24h = fetch_cash_yield_metrics()
        results["lifetime"]["cash_yield"] = lifetime_yield
        results["24h"]["cash_yield"] = yield_24h
    except Exception as e:
        print(f"❌ Error fetching cash yield: {e}")

    return results
