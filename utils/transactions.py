import json
from decimal import Decimal
from helpers.constants import CHAIN_ID_MAP


def parse_txn(txn_raw):
    try:
        return txn_raw if isinstance(txn_raw, dict) else json.loads(txn_raw)
    except:
        return {}

def safe_decimal(val):
    try:
        return Decimal(str(val))
    except:
        return Decimal(0)

def normalize(amount, price, decimals):
    try:
        return float(safe_decimal(amount) * safe_decimal(price) / (10 ** int(decimals)))
    except:
        return 0

def get_chain_ids(txn, activity_chain_ids):
    if isinstance(activity_chain_ids, list):
        if len(activity_chain_ids) == 2:
            return int(activity_chain_ids[0]), int(activity_chain_ids[1])
        elif len(activity_chain_ids) == 1:
            return int(activity_chain_ids[0]), int(activity_chain_ids[0])

    chain_id = txn.get("chainId")
    if chain_id:
        return int(chain_id), int(chain_id)

    from_id = txn.get("fromChainId") or txn.get("route", {}).get("fromChainId")
    to_id = txn.get("toChainId") or txn.get("route", {}).get("toChainId")

    try:
        from_id = int(from_id)
    except:
        from_id = None
    try:
        to_id = int(to_id)
    except:
        to_id = from_id

    return from_id, to_id

def resolve_username_by_userid(user_id, conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT username FROM "User" WHERE "userId" = %s LIMIT 1', (user_id,))
            row = cursor.fetchone()
            return row[0] if row and row[0] else user_id
    except Exception:
        return user_id

def resolve_username_by_address(address, conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                SELECT u.username
                FROM "Wallet" w
                JOIN "WalletAccount" wa ON w."walletAccountId" = wa."id"
                JOIN "User" u ON wa."userId" = u."userId"
                WHERE LOWER(w.address) = LOWER(%s)
                LIMIT 1
            ''', (address,))
            row = cursor.fetchone()
            return row[0] if row and row[0] else address
    except Exception:
        return address

def transform_activity_transaction(
    tx_hash,
    txn_raw,
    typ,
    status,
    created_at,
    user_id,
    conn,
    chain_ids=None
):
    try:
        txn = json.loads(txn_raw) if isinstance(txn_raw, str) else txn_raw
    except Exception as e:
        print(f"❌ Failed to parse txn JSON: {e}")
        return None

    from_user = resolve_username_by_userid(user_id, conn)
    to_user = None
    from_token = to_token = from_chain = to_chain = None
    amount_usd = 0

    from_chain_id, to_chain_id = get_chain_ids(txn, chain_ids)
    from_chain = CHAIN_ID_MAP.get(from_chain_id, str(from_chain_id))
    to_chain = CHAIN_ID_MAP.get(to_chain_id, str(to_chain_id))

    if typ == "CASH":
        sub_status = txn.get("subStatus")
        token = txn.get("token", {})
        amount_usd = float(txn.get("amount", 0))
        from_token = to_token = token.get("symbol")

        if sub_status in ("SEND", "RECEIVE"):
            from_user_id = txn.get("fromUserId")
            to_user_id = txn.get("toUserId")
            from_user = resolve_username_by_userid(from_user_id, conn) if from_user_id else None
            to_user = resolve_username_by_userid(to_user_id, conn) if to_user_id else None
        elif sub_status == "CONVERT":
            convert_type = txn.get("type")
            to_user = {
                "CASH_TO_CRYPTO": "CONVERT: CASH TO CRYPTO",
                "CRYPTO_TO_CASH": "CONVERT: CRYPTO TO CASH"
            }.get(convert_type, "CONVERT")
        elif sub_status in ("DEPOSIT", "WITHDRAW"):
            to_user = sub_status
        else:
            to_user = txn.get("toUsername") or txn.get("toExternalUser") or txn.get("toUserId")

    elif typ == "SEND":
        token = txn.get("token", {})
        amount_usd = normalize(txn.get("amount", 0), token.get("tokenPrices", {}).get("usd", 1), token.get("decimals", 18))
        from_token = to_token = token.get("symbol")
        to_user = resolve_username_by_address(txn.get("toAddress"), conn)

    elif typ in ("SWAP", "BRIDGE"):
        from_meta = txn.get("fromToken") or txn.get("route", {}).get("fromToken", {})
        to_meta = txn.get("toToken") or txn.get("route", {}).get("toToken", {})
        from_token = from_meta.get("symbol")
        to_token = to_meta.get("symbol")
        from_amt = txn.get("fromAmount", 0)
        price = from_meta.get("tokenPrices", {}).get("usd") or from_meta.get("priceUSD") or 1
        decimals = from_meta.get("decimals", 18)
        amount_usd = normalize(from_amt, price, decimals)
        to_user = from_user

    return {
        "created_at": created_at,
        "type": typ,
        "status": status,
        "from_user": from_user,
        "to_user": to_user,
        "from_token": from_token,
        "to_token": to_token,
        "from_chain": from_chain,
        "to_chain": to_chain,
        "amount_usd": min(amount_usd, 999999.99),
        "chain_id": from_chain_id,
        "tx_hash": tx_hash,
        "raw_transaction": txn
    }