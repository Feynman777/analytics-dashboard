from datetime import datetime
from helpers.api_utils import fetch_api_metric
from helpers.connection import get_main_db_connection


def fetch_user_profile_summary(conn, identifier: str):
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
            "createdAt": row[3]
        }


def fetch_user_metrics_full(user_identifier: str, start=None, end=None):
    if not user_identifier:
        return {}

    # Fetch user wallet addresses
    with get_main_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    MAX(CASE WHEN chain = 'evm' THEN w.address ELSE NULL END) AS evm,
                    MAX(CASE WHEN chain = 'solana' THEN w.address ELSE NULL END) AS solana,
                    MAX(CASE WHEN chain = 'btc' THEN w.address ELSE NULL END) AS btc,
                    MAX(CASE WHEN chain = 'sui' THEN w.address ELSE NULL END) AS sui
                FROM "Wallet" w
                JOIN "WalletAccount" wa ON w."walletAccountId" = wa."id"
                JOIN "User" u ON wa."userId" = u."userId"
                WHERE LOWER(u.username) = LOWER(%s) OR LOWER(u.email) = LOWER(%s)
            """, (user_identifier, user_identifier))
            row = cur.fetchone()
            wallets = {
                "evm": row[0],
                "solana": row[1],
                "btc": row[2],
                "sui": row[3]
            }

    # Fetch API-based metrics
    cash = fetch_api_metric("user/metrics/cash", username=user_identifier)
    volume_all = fetch_api_metric("user/metrics/volume", username=user_identifier)
    refs_all = fetch_api_metric("user/referrals", username=user_identifier)

    volume_filtered = fetch_api_metric("user/metrics/volume", username=user_identifier, start=start, end=end)
    refs_filtered = fetch_api_metric("user/referrals", username=user_identifier, start=start, end=end)

    return {
        "profile": wallets,
        "cash": cash.iloc[0].to_dict() if not cash.empty else {},
        "lifetime": {
            "volume": volume_all.iloc[0].to_dict() if not volume_all.empty else {},
            "referrals": int(refs_all.iloc[0]["value"]) if not refs_all.empty else 0,
        },
        "filtered": {
            "volume": volume_filtered.iloc[0].to_dict() if not volume_filtered.empty else {},
            "referrals": int(refs_filtered.iloc[0]["value"]) if not refs_filtered.empty else 0,
        }
    }
