import pandas as pd
from datetime import datetime
from psycopg2.extras import RealDictCursor
from collections import defaultdict

from helpers.connection import get_cache_db_connection, get_main_db_connection


def fetch_daily_user_stats(start=None, end=None) -> pd.DataFrame:
    query = "SELECT * FROM daily_user_stats WHERE 1=1"
    params = []

    if start:
        query += " AND date >= %s"
        params.append(start)
    if end:
        query += " AND date <= %s"
        params.append(end)

    query += " ORDER BY date ASC"

    with get_cache_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def fetch_user_volume_by_day(username: str) -> pd.DataFrame:
    query = """
        SELECT created_at, amount_usd
        FROM transactions_cache
        WHERE type = 'SWAP' AND status = 'SUCCESS' AND from_user = %s
    """

    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (username,))
            rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["created_at", "amount_usd"])
    df["date"] = pd.to_datetime(df["created_at"]).dt.date
    df = df.groupby("date")["amount_usd"].sum().reset_index()
    df.columns = ["date", "daily_volume_usd"]
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


def fetch_user_txn_timeseries(username: str) -> pd.DataFrame:
    query = """
        SELECT created_at, amount_usd
        FROM transactions_cache
        WHERE type = 'SWAP' AND status = 'SUCCESS' AND from_user = %s
    """

    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (username,))
            rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=["datetime", "volume_usd"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime")


def fetch_all_users() -> pd.DataFrame:
    query = """
        SELECT "userId", username, "createdAt"
        FROM "User"
        WHERE username IS NOT NULL
    """

    with get_main_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return pd.DataFrame(cur.fetchall())


def fetch_top_users_last_7d(conn, limit=25):
    query = """
        SELECT from_user, SUM(amount_usd) AS total_volume
        FROM transactions_cache
        WHERE type = 'SWAP' AND status = 'SUCCESS'
          AND created_at >= NOW() - INTERVAL '7 days'
        GROUP BY from_user
        ORDER BY total_volume DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (limit,))
        return cur.fetchall()


def get_user_daily_volume(username: str):
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT created_at, amount_usd
                    FROM transactions_cache
                    WHERE type = 'SWAP'
                      AND status = 'SUCCESS'
                      AND from_user = %s
                """, (username,))
                rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(), pd.DataFrame()

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["created_at"]).dt.date
        df_day = df.groupby("date")["amount_usd"].sum().reset_index()
        df_day.columns = ["date", "daily_volume_usd"]
        df_day["date"] = pd.to_datetime(df_day["date"])

        df_ts = df[["created_at", "amount_usd"]].copy()
        df_ts.columns = ["datetime", "volume_usd"]
        df_ts["datetime"] = pd.to_datetime(df_ts["datetime"])

        return df_day.sort_values("date"), df_ts.sort_values("datetime")

    except Exception as e:
        print(f"❌ Error loading volume for user `{username}`: {e}")
        return pd.DataFrame(), pd.DataFrame()


def get_referral_mapping():
    """
    Returns { referred_username: referrer_username } from the main DB.
    """
    with get_main_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.username AS referred_username, u.username AS referrer_username
                FROM "User" r
                JOIN "User" u ON r."referredBy" = u."referId"
                WHERE r.username IS NOT NULL AND u.username IS NOT NULL
            """)
            return dict(cur.fetchall())


def fetch_top_users_by_metric(conn, metric="swap", start_date=None, end_date=None, chains=None, limit=50):
    metric = metric.lower()

    if metric == "referrals":
        referral_map = get_referral_mapping()
        if not referral_map:
            return []

        # Step 1: Get volume for referred users
        with conn.cursor() as cur:
            where_clauses = ["status = 'SUCCESS'", "type = 'SWAP'", "from_user = ANY(%s)"]
            params = [list(referral_map.keys())]

            if start_date:
                where_clauses.append("created_at >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("created_at <= %s")
                params.append(end_date)
            if chains:
                where_clauses.append("from_chain = ANY(%s)")
                params.append(chains)

            query = f"""
                SELECT from_user, SUM(amount_usd)
                FROM transactions_cache
                WHERE {" AND ".join(where_clauses)}
                GROUP BY from_user
            """
            cur.execute(query, params)
            referred_volumes = {user: float(vol) for user, vol in cur.fetchall()}

        # Step 2: Aggregate by referrer
        aggregated = defaultdict(lambda: {"count": 0, "volume": 0.0})
        for referred_user, referrer in referral_map.items():
            aggregated[referrer]["count"] += 1
            if referred_user in referred_volumes:
                aggregated[referrer]["volume"] += referred_volumes[referred_user]

        # Step 3: Sort and return top N
        top = sorted(aggregated.items(), key=lambda x: x[1]["volume"], reverse=True)[:limit]
        return [(referrer, data) for referrer, data in top]

    # === SWAP or CASH logic ===
    if metric == "swap":
        filter_clause = "type = 'SWAP'"
        value_col = "amount_usd"
    elif metric == "cash":
        filter_clause = "type = 'CASH'"
        value_col = "amount_usd"
    else:
        raise ValueError(f"Unsupported leaderboard metric: {metric}")

    query = f"""
        SELECT from_user AS username, SUM({value_col}) AS total
        FROM transactions_cache
        WHERE status = 'SUCCESS' AND from_user IS NOT NULL AND {filter_clause}
    """
    params = []

    if start_date:
        query += " AND created_at >= %s"
        params.append(start_date)
    if end_date:
        query += " AND created_at <= %s"
        params.append(end_date)
    if chains:
        query += " AND from_chain = ANY(%s)"
        params.append(chains)

    query += " GROUP BY from_user ORDER BY total DESC LIMIT %s"
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()
