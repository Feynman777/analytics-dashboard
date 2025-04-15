import json
from decimal import Decimal
import hashlib
from helpers.constants import CHAIN_ID_MAP

def safe_float(val, default=0.0):
    try:
        return float(val or default)
    except (ValueError, TypeError):
        return default

def safe_decimal(val, default=Decimal(0)):
    try:
        return Decimal(val or 0)
    except (ValueError, TypeError, ArithmeticError):
        return default

def format_dapp_tx_display(txn_raw):
    try:
        txn = json.loads(txn_raw) if isinstance(txn_raw, str) else txn_raw

        # Extract hostname
        site_info = txn.get("site", {})
        host = site_info.get("host", "unknown")

        # Use result hash if present
        result_hash = txn.get("result")
        if isinstance(result_hash, str) and result_hash.startswith("0x"):
            short_hash = result_hash[2:10]
        else:
            # Fallback: hash of raw txn
            raw_str = json.dumps(txn, sort_keys=True)
            short_hash = hashlib.sha256(raw_str.encode()).hexdigest()[:8]

        return f"{host} - {short_hash}"

    except Exception as e:
        print(f"[ERROR] Failed to format DAPP tx display: {e}")
        return "unknown - errorhash"

def generate_fallback_tx_hash(created_at, txn_raw):
    # Ensure txn_raw is a string (e.g. handle int, dict, None)
    if not isinstance(txn_raw, str):
        txn_raw = str(txn_raw)

    hash_obj = hashlib.sha256(txn_raw.encode())
    hash_digest = hash_obj.hexdigest()[:8]
    return f"unknown-{created_at.strftime('%Y%m%d%H%M%S')}-{hash_digest}"

def parse_txn_json(txn_raw):
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
    chain_ids=None,
    existing=None
):
    from_user = resolve_username_by_userid(user_id, conn)
    to_user = None
    from_token = to_token = from_chain = to_chain = None
    amount_usd = 0
    fee_usd = 0
    tx_display = None


    # Parse transaction safely
    try:
        txn = json.loads(txn_raw) if isinstance(txn_raw, str) else txn_raw
    except Exception as e:
        print(f"❌ Failed to parse txn JSON: {e}")
        return None

    # Patch SUI FAILs to SUCCESS — but skip if tx_hash was originally None
    if typ == "SWAP":
        print(f"type = {typ}")
        print("✅ yes - swap")
        print(f"tx_hash = {tx_hash}")
        if not tx_hash:
            print("❌ tx_hash was None originally — skip patching this SUI SWAP")
        if chain_ids and 2 in chain_ids and status == "FAIL" and tx_hash and not tx_hash.startswith("unknown"):
            print(f"[PATCH] Corrected SUI txn {tx_hash} from FAIL to SUCCESS")
            status = "SUCCESS"
    else:
        print(f"type = {typ}")
        print("❌ no - not swap")

    # Chain mapping
    from_chain_id, to_chain_id = get_chain_ids(txn, chain_ids)
    from_chain = CHAIN_ID_MAP.get(from_chain_id, str(from_chain_id))
    to_chain = CHAIN_ID_MAP.get(to_chain_id, str(to_chain_id))

    # === SEND Transaction ===
    if typ == "SEND":
        from_meta = txn.get("fromToken") or txn.get("route", {}).get("fromToken", {}) or txn.get("token", {})
        to_meta = txn.get("toToken") or txn.get("route", {}).get("toToken", {}) or from_meta

        from_token = from_meta.get("symbol")
        to_token = to_meta.get("symbol")

        amount_raw = txn.get("amount") or 0
        price = from_meta.get("tokenPrices", {}).get("usd") or from_meta.get("priceUSD") or 1
        decimals = int(from_meta.get("decimals", 18))

        try:
            amount_usd = safe_float(Decimal(amount_raw) * Decimal(price) / Decimal(10 ** decimals))
        except Exception as e:
            print(f"[WARN] Failed to compute amount_usd for SEND {tx_hash}: {e}")
            amount_usd = 0

        to_user = txn.get("toUsername") or txn.get("toUser") or from_user

    # === SWAP or BRIDGE Transaction ===
    elif typ in ("SWAP", "BRIDGE"):
        from_meta = txn.get("fromToken") or txn.get("route", {}).get("fromToken", {})
        to_meta = txn.get("toToken") or txn.get("route", {}).get("toToken", {})

        from_token = from_meta.get("symbol")
        to_token = to_meta.get("symbol")

        from_amt = safe_decimal(txn.get("fromAmount", 0))
        price = safe_decimal(from_meta.get("tokenPrices", {}).get("usd") or from_meta.get("priceUSD") or 0)
        decimals = int(from_meta.get("decimals", 18))

        try:
            amount_usd = safe_float(from_amt * price / Decimal(10 ** decimals))
        except Exception as e:
            print(f"[WARN] normalize-like fallback failed in {tx_hash}: {e}")
            amount_usd = 0

        to_user = from_user

        # === SUI-style fee ===
        nm_fee = txn.get("route", {}).get("nmFee", {})
        if nm_fee and "amount" in nm_fee:
            try:
                amount = safe_decimal(nm_fee.get("amount"))
                token = nm_fee.get("token", {})
                price_usd = safe_decimal(token.get("tokenPrices", {}).get("usd"))
                decimals = int(token.get("decimals", 18))
                value_usd = safe_float(amount * price_usd / Decimal(10 ** decimals))
                fee_usd += value_usd
            except Exception as e:
                print(f"[WARN] Failed to parse SUI fee for {tx_hash}: {e}")

        # === LIFI-style steps/fees ===
        steps = txn.get("route", {}).get("steps", [])
        for step in steps:
            estimate = step.get("estimate", {})
            fee_costs = estimate.get("feeCosts", [])
            for fee in fee_costs:
                try:
                    amount = safe_decimal(fee.get("amount"))
                    token = fee.get("token", {})
                    price_usd = safe_decimal(token.get("priceUSD"))
                    decimals = int(token.get("decimals", 18))
                    value_usd = safe_float(amount * price_usd / Decimal(10 ** decimals))
                    fee_usd += value_usd
                except Exception as e:
                    print(f"[WARN] Failed to parse LIFI fee for {tx_hash}: {e}")

    # === DAPP Transaction ===
    elif typ == "DAPP":
        tx_display = format_dapp_tx_display(txn_raw)

    # === CASH Transaction ===
    elif typ == "CASH":
        amount_usd = safe_float(txn.get("amount", 0))
        fee_usd = safe_float(txn.get("fee", 0))

        token_meta = txn.get("token", {})
        from_token = to_token = token_meta.get("symbol", "USD")
        to_user = from_user

    if typ == "SWAP" and chain_ids and 2 in chain_ids:
        print(f"[SUI SWAP] Final status: {status} | tx_hash: {tx_hash}")

    print(f"[FINAL] {tx_hash} | {typ} | {status} | {amount_usd}")

    # 🚨 FINAL status override for fallback SWAPs
    if typ == "SWAP" and (not tx_hash or tx_hash.startswith("unknown")):
        status = "FAIL"
        print(f"🛡️ OVERRIDE: Enforced FAIL status for fallback SWAP tx_hash={tx_hash}")

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
        "raw_transaction": txn,
        "tx_display": tx_display,
    }
