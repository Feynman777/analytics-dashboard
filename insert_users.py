import pandas as pd
from psycopg2.extras import execute_values
from helpers.connection import get_main_db_connection, get_cache_db_connection

def fetch_main_users():
    print("🔁 Fetching all users from main DB...")
    query = """
        SELECT "userId", username, "createdAt"
        FROM "User"
        WHERE username IS NOT NULL
    """
    with get_main_db_connection() as conn:
        return pd.read_sql(query, conn)

def fetch_first_active_dates():
    print("🔍 Fetching first active dates from cache DB...")
    query = """
        SELECT from_user AS username, MIN(created_at) AS first_active_at
        FROM transactions_cache
        WHERE status = 'SUCCESS' AND from_user IS NOT NULL
        GROUP BY from_user
    """
    with get_cache_db_connection() as conn:
        return pd.read_sql(query, conn)

def insert_users(users_df: pd.DataFrame):
    users_df["createdAt"] = pd.to_datetime(users_df["createdAt"], errors="coerce")
    users_df["first_active_at"] = pd.to_datetime(users_df["first_active_at"], errors="coerce")

    print(f"📊 Prepared {len(users_df)} users for insertion.")
    print("🔎 Example row:", users_df.iloc[0].to_dict())

    # Explicitly convert NaT to None for psycopg2 compatibility
    values = []
    for _, row in users_df.iterrows():
        created_at = row["createdAt"] if pd.notnull(row["createdAt"]) else None
        first_active_at = row["first_active_at"] if pd.notnull(row["first_active_at"]) else None
        values.append((row["userId"], row["username"], created_at, first_active_at))

    with get_cache_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO users (user_id, username, created_at, first_active_at)
                VALUES %s
                ON CONFLICT (user_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    created_at = EXCLUDED.created_at,
                    first_active_at = EXCLUDED.first_active_at
            """, values)
        conn.commit()
        print("✅ Users inserted successfully.")

if __name__ == "__main__":
    users = fetch_main_users()
    active_dates = fetch_first_active_dates()

    print("🔗 Merging...")
    merged = pd.merge(users, active_dates, how="left", on="username")
    insert_users(merged)
