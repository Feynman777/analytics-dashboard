import json
import hashlib
from decimal import Decimal
from helpers.utils.constants import CHAIN_ID_MAP

# === General Helpers ===
def safe_float(val, default=0.0):
    try:
        return float(val or default)
    except (ValueError, TypeError):
        return default

def safe_decimal(val, default=Decimal(0)):
    try:
        return Decimal(str(val))
    except (ValueError, TypeError):
        return default

def normalize(amount, price, decimals):
    try:
        return float(safe_decimal(amount) * safe_decimal(price) / Decimal(10 ** int(decimals)))
    except Exception:
        return 0

# === Fallback & Hashing ===
def generate_fallback_tx_hash(created_at, txn_raw):
    if not isinstance(txn_raw, str):
        txn_raw = str(txn_raw)
    digest = hashlib.sha256(txn_raw.encode()).hexdigest()[:8]
    return f"unknown-{created_at.strftime('%Y%m%d%H%M%S')}-{digest}"

# === Username Resolution ===
def resolve_username_by_userid(user_id, conn):
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT username FROM "User" WHERE "userId" = %s LIMIT 1', (user_id,))
            row = cur.fetchone()
            return row[0] if row and row[0] else user_id
    except Exception:
        return user_id

def resolve_username_by_address(address, conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.username
                FROM "Wallet" w
                JOIN "WalletAccount" wa ON w."walletAccountId" = wa."id"
                JOIN "User" u ON wa."userId" = u."userId"
                WHERE LOWER(w.address) = LOWER(%s)
                LIMIT 1
            """, (address,))
            row = cur.fetchone()
            return row[0] if row and row[0] else address
    except Exception:
        return address

# === Chain Resolution ===
def get_chain_ids(txn, activity_chain_ids):
    if isinstance(activity_chain_ids, list):
        if len(activity_chain_ids) == 2:
            return int(activity_chain_ids[0]), int(activity_chain_ids[1])
        if len(activity_chain_ids) == 1:
            return int(activity_chain_ids[0]), int(activity_chain_ids[0])

    from_id = txn.get("fromChainId") or txn.get("route", {}).get("fromChainId") or txn.get("chainId")
    to_id = txn.get("toChainId") or txn.get("route", {}).get("toChainId") or from_id

    try:
        return int(from_id), int(to_id)
    except Exception:
        return None, None

# === DAPP Display ===
def format_dapp_tx_display(txn_raw):
    try:
        txn = json.loads(txn_raw) if isinstance(txn_raw, str) else txn_raw
        host = txn.get("site", {}).get("host", "unknown")
        result_hash = txn.get("result")

        short_hash = (
            result_hash[2:10] if isinstance(result_hash, str) and result_hash.startswith("0x")
            else hashlib.sha256(json.dumps(txn, sort_keys=True).encode()).hexdigest()[:8]
        )

        return f"{host} - {short_hash}"
    except Exception:
        return "unknown - errorhash"

# === Transaction Parser ===
def parse_txn_json(txn_raw):
    try:
        return txn_raw if isinstance(txn_raw, dict) else json.loads(txn_raw)
    except Exception:
        return {}

# === Core Transform ===
def transform_activity_transaction(tx_hash, txn_raw, typ, status, created_at, user_id, conn, chain_ids=None, existing=None):
    from_user = resolve_username_by_userid(user_id, conn)
    to_user = None
    from_token = to_token = from_chain = to_chain = None
    amount_usd = fee_usd = 0
    tx_display = None

    txn = parse_txn_json(txn_raw)
    if not txn:
        return None

    from_chain_id, to_chain_id = get_chain_ids(txn, chain_ids)
    from_chain = CHAIN_ID_MAP.get(from_chain_id, str(from_chain_id))
    to_chain = CHAIN_ID_MAP.get(to_chain_id, str(to_chain_id))

    if typ == "SEND":
        token = txn.get("fromToken") or txn.get("route", {}).get("fromToken") or txn.get("token", {})
        from_token = to_token = token.get("symbol")
        amount = txn.get("amount", 0)
        price = token.get("tokenPrices", {}).get("usd") or token.get("priceUSD") or 1
        decimals = int(token.get("decimals", 18))
        amount_usd = normalize(amount, price, decimals)
        to_user = txn.get("toUsername") or txn.get("toUser") or from_user

    elif typ in ("SWAP", "BRIDGE"):
        from_meta = txn.get("fromToken") or txn.get("route", {}).get("fromToken", {})
        to_meta = txn.get("toToken") or txn.get("route", {}).get("toToken", {})
        from_token = from_meta.get("symbol")
        to_token = to_meta.get("symbol")

        from_amt = txn.get("fromAmount", 0)
        price = from_meta.get("tokenPrices", {}).get("usd") or from_meta.get("priceUSD") or 0
        decimals = int(from_meta.get("decimals", 18))
        amount_usd = normalize(from_amt, price, decimals)
        to_user = from_user

        # SUI fee format
        sui_fee = txn.get("nmFee") or txn.get("route", {}).get("nmFee", {})
        if "amount" in sui_fee:
            try:
                fee_amt = safe_decimal(sui_fee["amount"])
                token = sui_fee.get("token", {})
                fee_price = safe_decimal(token.get("tokenPrices", {}).get("usd"))
                fee_decimals = int(token.get("decimals", 18))
                fee_usd += safe_float(fee_amt * fee_price / Decimal(10 ** fee_decimals))
            except Exception:
                pass

        # LIFI fee format
        for step in txn.get("route", {}).get("steps", []):
            for fee in step.get("estimate", {}).get("feeCosts", []):
                try:
                    amt = safe_decimal(fee["amount"])
                    token = fee.get("token", {})
                    price = safe_decimal(token.get("priceUSD"))
                    decimals = int(token.get("decimals", 18))
                    fee_usd += safe_float(amt * price / Decimal(10 ** decimals))
                except Exception:
                    pass

    elif typ == "DAPP":
        tx_display = format_dapp_tx_display(txn_raw)
        to_user = from_user

    elif typ == "CASH":
        if txn.get("subStatus") != "SEND":
            return None
        amount_usd = safe_float(txn.get("amount", 0))
        fee_usd = safe_float(txn.get("fee", 0))
        token = txn.get("token", {})
        from_token = to_token = token.get("symbol", "USD")
        to_user_id = txn.get("toUserId")
        to_user = (
            resolve_username_by_userid(to_user_id, conn)
            or txn.get("toUsername")
            or txn.get("toExternalUser")
            or to_user_id
            or "N/A"
        )

    # Fallback tx_hash if missing
    if not tx_hash or str(tx_hash).lower() in ("null", "none"):
        tx_hash = generate_fallback_tx_hash(created_at, txn)

    # Never falsely promote unknown SWAP tx_hashes to success
    if typ == "SWAP" and tx_hash.startswith("unknown"):
        status = "FAIL"

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
        "fee_usd": round(fee_usd, 8),
        "chain_id": from_chain_id,
        "tx_hash": tx_hash,
        "tx_display": tx_display,
    }

def sanitize_username(username):
    """Returns a safe fallback username if value is None, null, or invalid."""
    if not username or str(username).lower() in ("none", "null", "nan"):
        return "unknown"
    return str(username).strip()
