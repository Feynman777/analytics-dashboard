# helpers/upsert/transactions.py

def upsert_transactions_from_activity(force=False, batch_size=100, start=None, end=None):
    from datetime import datetime, timedelta, timezone
    from helpers.connection import get_main_db_connection, get_cache_db_connection
    from helpers.utils.transactions import transform_activity_transaction
    from helpers.utils.safe_math import safe_float

    main_conn = get_main_db_connection()
    cache_conn = get_cache_db_connection()

    with main_conn.cursor() as cur_main, cache_conn.cursor() as cur_cache:
        if start:
            sync_start = start
        elif force:
            sync_start = datetime.now(timezone.utc) - timedelta(hours=2)
        else:
            cur_cache.execute("SELECT MAX(created_at) FROM transactions_cache")
            latest_cached = cur_cache.fetchone()[0]
            sync_start = latest_cached - timedelta(hours=2) if latest_cached else datetime(2025, 4, 14, tzinfo=timezone.utc)

        sync_end = end or datetime.now(timezone.utc)

        cur_main.execute("""
            SELECT COUNT(*) FROM "Activity"
            WHERE "createdAt" >= %s AND "createdAt" < %s
        """, (sync_start, sync_end))
        total_rows = cur_main.fetchone()[0]

        insert_count = 0

        for offset in range(0, total_rows, batch_size):
            cur_main.execute("""
                SELECT "createdAt", "userId", type, status, hash, transaction, "chainIds"
                FROM "Activity"
                WHERE "createdAt" >= %s AND "createdAt" < %s
                ORDER BY "createdAt" ASC
                LIMIT %s OFFSET %s
            """, (sync_start, sync_end, batch_size, offset))

            rows = cur_main.fetchall()
            if not rows:
                break

            for created_at, user_id, typ, status, tx_hash, txn_raw, chain_ids in rows:
                try:
                    tx_data = transform_activity_transaction(
                        tx_hash=tx_hash,
                        txn_raw=txn_raw,
                        typ=typ,
                        status=status,
                        created_at=created_at,
                        user_id=user_id,
                        conn=main_conn,
                        chain_ids=chain_ids
                    )

                    if not tx_data or not tx_data.get("tx_hash"):
                        continue

                    if tx_data["type"] == "SWAP" and tx_data["tx_hash"].startswith("unknown-"):
                        tx_data["status"] = "FAIL"

                    to_user = tx_data.get("to_user")
                    if isinstance(to_user, dict):
                        to_user = to_user.get("username") or "unknown"

                    tx_display = tx_data.get("tx_display")
                    if isinstance(tx_display, dict):
                        tx_display = tx_display.get("text") or str(tx_display)

                    cur_cache.execute("""
                        INSERT INTO transactions_cache (
                            created_at, type, status, from_user, to_user,
                            from_token, from_chain, to_token, to_chain,
                            amount_usd, fee_usd, tx_hash, chain_id, tx_display
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tx_hash) DO UPDATE SET
                            amount_usd = EXCLUDED.amount_usd,
                            fee_usd = EXCLUDED.fee_usd,
                            to_user = EXCLUDED.to_user,
                            from_user = EXCLUDED.from_user,
                            from_token = EXCLUDED.from_token,
                            to_token = EXCLUDED.to_token,
                            from_chain = EXCLUDED.from_chain,
                            to_chain = EXCLUDED.to_chain,
                            status = EXCLUDED.status,
                            tx_display = EXCLUDED.tx_display,
                            created_at = EXCLUDED.created_at
                    """, (
                        tx_data["created_at"], tx_data["type"], tx_data["status"],
                        tx_data["from_user"], to_user,
                        tx_data["from_token"], tx_data["from_chain"],
                        tx_data["to_token"], tx_data["to_chain"],
                        safe_float(tx_data.get("amount_usd")), safe_float(tx_data.get("fee_usd")),
                        tx_data["tx_hash"], tx_data["chain_id"], tx_display
                    ))

                    insert_count += 1

                except Exception as e:
                    print(f"❌ Error processing transaction: {e}")
                    continue

            cache_conn.commit()

