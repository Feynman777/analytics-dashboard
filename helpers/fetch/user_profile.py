from datetime import datetime
import pandas as pd
from helpers.api_utils import fetch_api_metric, fetch_api_json, fetch_api_raw
from helpers.connection import get_main_db_connection

def fetch_user_profile_summary(conn, identifier: str) -> dict | None:
    """
    Attempts to match user by username, email, or wallet address.
    Returns basic profile metadata if found.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT u."userId", u.username, u.email, u."createdAt"
            FROM "User" u
            WHERE LOWER(u.username) = LOWER(%s)
               OR LOWER(u.email) = LOWER(%s)
               OR EXISTS (
                   SELECT 1 FROM "Wallet" w
                   JOIN "WalletAccount" wa ON w."walletAccountId" = wa."id"
                   WHERE wa."userId" = u."userId" AND LOWER(w.address) = LOWER(%s)
               )
            LIMIT 1
        """, (identifier, identifier, identifier))
        row = cur.fetchone()

    if not row:
        return None

    return {
        "userId": row[0],
        "username": row[1],
        "email": row[2],
        "createdAt": row[3],
    }


def fetch_user_metrics_full(user_identifier: str, start: str = None, end: str = None) -> dict:
    if not user_identifier:
        return {}

    # === Wallet Addresses ===
    with get_main_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    MAX(CASE WHEN "chainType" = 'ETHEREUM' THEN w.address END) as evm,
                    MAX(CASE WHEN "chainType" = 'SOLANA' THEN w.address END) as solana,
                    MAX(CASE WHEN "chainType" = 'BITCOIN' THEN w.address END) as btc,
                    MAX(CASE WHEN "chainType" = 'SUI' THEN w.address END) as sui
                FROM "Wallet" w
                JOIN "WalletAccount" wa ON w."walletAccountId" = wa."id"
                JOIN "User" u ON wa."userId" = u."userId"
                WHERE LOWER(u.username) = LOWER(%s) OR LOWER(u.email) = LOWER(%s)
            """, (user_identifier, user_identifier))
            row = cur.fetchone()
            wallets = dict(zip(["evm", "solana", "btc", "sui"], row or [None] * 4))

    # === Full profile ===
    try:
        df = fetch_api_metric(f"user/metrics/{user_identifier}")
        if df.empty:
            return {"profile": wallets}
        metrics = df.iloc[0].to_dict()
    except Exception as e:
        print(f"❌ Failed to fetch full metrics: {e}")
        return {"profile": wallets}

    # === Filtered volume (dict from JSON) ===
    def fetch_filtered_volume():
        url = f"user/metrics/volume/{user_identifier}?start={start}" if start else f"user/metrics/volume/{user_identifier}"
        try:
            return fetch_api_json(url)
        except Exception as e:
            print(f"❌ Error fetching filtered volume: {e}")
            return {}

    # === Filtered referrals (raw integer) ===
    def fetch_filtered_referrals():
        url = f"user/referrals/{user_identifier}?start={start}" if start else f"user/referrals/{user_identifier}"
        try:
            return int(fetch_api_raw(url))
        except Exception as e:
            print(f"❌ Error fetching filtered referrals: {e}")
            return 0

    return {
        "profile": wallets,
        "cash": metrics.get("cash", {}),
        "crypto": metrics.get("crypto", {}),
        "lifetime": {
            "volume": metrics.get("crypto", {}).get("swaps", {}),
            "referrals": metrics.get("referrals", 0),
        },
        "filtered": {
            "volume": fetch_filtered_volume(),
            "referrals": fetch_filtered_referrals(),
        }
    }
