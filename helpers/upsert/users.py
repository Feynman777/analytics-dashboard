from datetime import datetime, timedelta
import pandas as pd
from psycopg2.extras import execute_values
from helpers.connection import get_main_db_connection, get_cache_db_connection

def upsert_users():
    today = datetime.utcnow().date()
    two_days_ago = today - timedelta(days=2)

    print(f"\n🔁 Fetching users created or active since {two_days_ago}...")

    query = """
        SELECT "userId", username, "createdAt"
        FROM "User"
        WHERE username IS NOT NULL AND "createdAt" >= %s
    """
    with get_main_db_connection() as conn:
        users_df = pd.read_sql(query, conn, params=(two_days_ago,))

    if users_df.empty:
        print("⚠️ No recently created users found.")
        return

    print("🔍 Fetching first active dates from recent transactions...")
    query = """
        SELECT from_user AS username, MIN(created_at) AS first_active_at
        FROM transactions_cache
        WHERE status = 'SUCCESS' AND created_at >= %s
        GROUP BY from_user
    """
    with get_cache_db_connection() as conn:
        active_df = pd.read_sql(query, conn, params=(two_days_ago,))

    print("🔗 Merging...")
    users_df = users_df.merge(active_df, on="username", how="left")
    users_df["createdAt"] = pd.to_datetime(users_df["createdAt"], errors="coerce")
    users_df["first_active_at"] = pd.to_datetime(users_df["first_active_at"], errors="coerce")
    users_df["createdAt"] = users_df["createdAt"].apply(lambda x: x if pd.notnull(x) else None)
    users_df["first_active_at"] = users_df["first_active_at"].apply(lambda x: x if pd.notnull(x) else None)

    print(f"📊 Prepared {len(users_df)} users for upsert.")

    if not users_df.empty:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO users (user_id, username, created_at, first_active_at)
                    VALUES %s
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        created_at = EXCLUDED.created_at,
                        first_active_at = EXCLUDED.first_active_at
                """, [
                    (
                        row["userId"],
                        row["username"],
                        row["createdAt"],
                        row["first_active_at"]
                    ) for _, row in users_df.iterrows()
                ])
            conn.commit()
        print(f"✅ Upserted {len(users_df)} users into users table.")
    else:
        print("⚠️ No users to upsert.")