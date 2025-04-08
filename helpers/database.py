#helpers/database.py
import psycopg2
import pandas as pd
from streamlit import secrets
from decimal import Decimal, getcontext
from io import StringIO
import time
import json
import datetime


# Set high precision for accurate token math
getcontext().prec = 30


# Fetch credentials from secrets.toml
DB_HOST_db = secrets["database"]["DB_HOST"]
DB_PORT_db = secrets["database"]["DB_PORT"]
DB_NAME_db = secrets["database"]["DB_NAME"]
DB_USER_db = secrets["database"]["DB_USER"]
DB_PASS_db = secrets["database"]["DB_PASS"]

DB_HOST_cache = secrets["cache_db"]["DB_HOST"]
DB_PORT_cache = secrets["cache_db"]["DB_PORT"]
DB_NAME_cache = secrets["cache_db"]["DB_NAME"]
DB_USER_cache = secrets["cache_db"]["DB_USER"]
DB_PASS_cache = secrets["cache_db"]["DB_PASS"]

CHAIN_ID_MAP = {
    8453: "base",
    42161: "arbitrum",
    137: "polygon",
    1: "ethereum",
    101: "solana",
    2: "sui",
    43114: "avalanche",
    34443: "mode",
    56: "bnb",
    10: "optimism"
}

def upsert_chain_timeseries(df):
    """
    Expects a DataFrame with columns:
    ['date', 'chain', 'metric', 'status', 'value', 'quantity']
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"Expected a DataFrame, got {type(df)}")

    # Ensure required columns are present
    required_columns = ['date', 'chain', 'metric', 'status', 'value', 'quantity']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Convert date to string format if it's a datetime object
    df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, (pd.Timestamp, datetime.date)) else x)

    # Ensure value and quantity are numeric
    df['value'] = df['value'].astype(float)
    df['quantity'] = df['quantity'].astype(int)

    try:
        with psycopg2.connect(
            host=DB_HOST_cache, port=int(DB_PORT_cache), dbname=DB_NAME_cache, user=DB_USER_cache, password=DB_PASS_cache
        ) as conn:
            with conn.cursor() as cursor:
                for _, row in df.iterrows():
                    #print(f"[DEBUG] Upserting row: date={row['date']}, chain={row['chain']}, value={row['value']}, quantity={row['quantity']}")
                    cursor.execute("""
                        INSERT INTO timeseries_chain_volume (date, chain, metric, status, value, quantity)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, chain, metric, status)
                        DO UPDATE SET value = EXCLUDED.value, quantity = EXCLUDED.quantity
                    """, (
                        row['date'],
                        row['chain'],
                        row['metric'],
                        row['status'],
                        row['value'],
                        row['quantity']
                    ))
                conn.commit()
                #print(f"[DEBUG] Upserted {len(df)} rows into timeseries_chain_volume")
    except Exception as e:
        print(f"[DEBUG] Error in upsert_chain_timeseries: {e}")
        raise

def get_cache_db_connection():
    """Establish the connection to the cache database."""
    conn = psycopg2.connect(
        host=DB_HOST_cache,
        port=int(DB_PORT_cache),
        database=DB_NAME_cache,
        user=DB_USER_cache,
        password=DB_PASS_cache
    )
    return conn

def upsert_timeseries(metric, df):
    """Upsert time series data into the database in batches with retry logic."""
    import time
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                batch_size = 50
                for start in range(0, len(df), batch_size):
                    batch = df.iloc[start:start + batch_size]
                    for attempt in range(3):
                        try:
                            for _, row in batch.iterrows():
                                date = row['date'].strftime('%Y-%m-%d')
                                value = float(row['value'])
                                #print(f"[DEBUG] Upserting {metric}: date={date}, value={value}")
                                cursor.execute("""
                                    INSERT INTO timeseries_cache (metric, date, value)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (metric, date) 
                                    DO UPDATE SET value = EXCLUDED.value
                                """, (metric, date, value))
                            conn.commit()
                            #print(f"[DEBUG] Committed {len(batch)} rows for {metric} in batch {start//batch_size + 1}")
                            break
                        except psycopg2.errors.DeadlockDetected as e:
                            #print(f"[DEBUG] Deadlock detected, retrying {attempt + 1}/3: {e}")
                            conn.rollback()
                            time.sleep(1)
                            if attempt == 2:
                                raise
    except Exception as e:
        print(f"❌ Error during upsert: {e}")
        conn.rollback()

def fetch_timeseries(metric, start_date=None, end_date=None):
    """Fetch time series data from the database."""
    try:
        with get_cache_db_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT date, value FROM timeseries_cache WHERE metric = %s"
                params = [metric]
                if start_date:
                    query += " AND date >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND date <= %s"
                    params.append(end_date)
                cursor.execute(query, tuple(params))
                data = cursor.fetchall()
                #print(f"[DEBUG] Fetched {len(data)} rows for {metric}")
                df = pd.DataFrame(data, columns=["date", "value"])
                df["date"] = pd.to_datetime(df["date"])
                return df
    except Exception as e:
        print(f"❌ Error during fetch: {e}")
        return pd.DataFrame()

def fetch_swap_series():
    supported_chains = {101, 1, 2, 42161, 137, 43114, 34443, 56, 8453, 10}
    skip_reasons = {
        "malformed_json": 0,
        "missing_chain_ids": 0,
        "invalid_amount": 0,
        "value_too_big": 0,
        "other_error": 0
    }
    try:
        with psycopg2.connect(
            host=DB_HOST_db, port=int(DB_PORT_db), dbname=DB_NAME_db, user=DB_USER_db, password=DB_PASS_db
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT "createdAt", transaction, "chainIds"
                    FROM public."Activity"
                    WHERE status = 'SUCCESS' 
                      AND type = 'SWAP'
                """)
                rows = cursor.fetchall()
                swap_data = {}
                skips = 0
                #print(f"[DEBUG] Fetched {len(rows)} swap txns")
                for created_at, txn_raw, chain_ids in rows:
                    try:
                        # Parse the transaction JSON using json.loads()
                        try:
                            transaction_json = json.loads(txn_raw)
                        except Exception as e:
                            skips += 1
                            skip_reasons["malformed_json"] += 1
                            #print(f"[DEBUG] Skipping txn at {created_at}: Malformed transaction JSON - {e}, Raw txn: {txn_raw}")
                            continue

                        # Extract fromChainId from chainIds (first element)
                        if not chain_ids or len(chain_ids) < 1:
                            skips += 1
                            skip_reasons["missing_chain_ids"] += 1
                            #print(f"[DEBUG] Skipping txn at {created_at}: Missing chainIds - chainIds: {chain_ids}, Raw txn: {txn_raw}")
                            continue
                        from_chain_id = chain_ids[0]  # Take the first element (fromChainId)

                        # Map chain ID to chain name
                        chain_name = CHAIN_ID_MAP.get(from_chain_id, str(from_chain_id))  # Fallback to ID if not in map

                        # Log if this is a supported chain
                        if from_chain_id in supported_chains:
                            print(f"[DEBUG] Processing supported chain at {created_at}: chainIds={chain_ids}, chainName={chain_name}, Raw txn: {txn_raw}")

                        # Try alternative keys for fromAmount
                        from_amount = transaction_json.get('fromAmount')
                        if from_amount is None:
                            from_amount = transaction_json.get('amount')
                        if from_amount is None:
                            from_amount = transaction_json.get('value')
                        from_amount = Decimal(from_amount if from_amount is not None else 0)

                        # Try alternative keys for fromToken
                        from_token = transaction_json.get('fromToken', {})
                        if not from_token:
                            from_token = transaction_json.get('token', {})
                        if not from_token:
                            from_token = transaction_json.get('sourceToken', {})

                        # Extract decimals
                        decimals = from_token.get('decimals')
                        if decimals is None:
                            decimals = from_token.get('decimal')
                        decimals = int(decimals if decimals is not None else 18)

                        # Extract price_usd
                        price_usd = from_token.get('tokenPrices', {}).get('usd')
                        if price_usd is None:
                            price_usd = from_token.get('price', {}).get('usd')
                        if price_usd is None:
                            price_usd = from_token.get('priceUSD')
                        price_usd = Decimal(price_usd if price_usd is not None else 0)

                        # Skip if from_amount is invalid
                        if from_amount <= 0:
                            skips += 1
                            skip_reasons["invalid_amount"] += 1
                            #print(f"[DEBUG] Skipping txn at {created_at}: Invalid fromAmount: {from_amount}, Raw txn: {txn_raw}")
                            continue

                        # Cap from_amount to prevent overflow
                        if from_amount > 1e50:  # Arbitrary large limit
                            from_amount = Decimal('1e50')
                            #print(f"[DEBUG] Capped from_amount at {created_at}: {from_amount}")

                        # Use default decimals if invalid
                        if decimals <= 0:
                            decimals = 18
                            #print(f"[DEBUG] Adjusted decimals to 18 at {created_at}: Original decimals was {decimals}")

                        # Handle missing price_usd
                        if price_usd <= 0:
                            price_usd = Decimal('0.01')  # Default to a small value
                            #print(f"[DEBUG] Set default price_usd to 0.01 at {created_at}: Original price_usd was {price_usd}")

                        # Cap price_usd to prevent overflow
                        if price_usd > 1e6:  # Unrealistic token price
                            price_usd = Decimal('1e6')
                            #print(f"[DEBUG] Capped price_usd at {created_at}: {price_usd}")

                        # Calculate volume with overflow protection
                        normalized = from_amount / Decimal(10 ** decimals)
                        volume = float(normalized * price_usd)

                        # Check for overflow
                        if volume > 1e308:
                            skips += 1
                            skip_reasons["value_too_big"] += 1
                            #print(f"[DEBUG] Skipping txn at {created_at}: Value too big! fromAmount={from_amount}, decimals={decimals}, price_usd={price_usd}, volume={volume}")
                            continue

                        # Aggregate the data
                        date_str = created_at.date().isoformat()
                        key = (date_str, chain_name, "swap_volume", "success")
                        if key in swap_data:
                            swap_data[key]["value"] += volume
                            swap_data[key]["quantity"] += 1
                        else:
                            swap_data[key] = {
                                "date": date_str,
                                "chain": chain_name,
                                "metric": "swap_volume",
                                "status": "success",
                                "value": volume,
                                "quantity": 1
                            }
                    except Exception as e:
                        skips += 1
                        skip_reasons["other_error"] += 1
                        #print(f"[DEBUG] Skipping txn at {created_at}: Error - {e}, chainIds: {chain_ids}, Raw txn: {txn_raw}")
                        continue

                result = list(swap_data.values())
                print(f"[DEBUG] Aggregated {len(result)} daily chain+status totals, skipped {skips} txns")
                print(f"[DEBUG] Skip reasons: {skip_reasons}")
                return result if result else []
    except Exception as e:
        print(f"[DEBUG] Database error in fetch_swap_series: {e}")
        return []