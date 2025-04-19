# cron_sync.py
from helpers.sync_utils import (
    sync_fee_series,
    sync_daily_stats,
    sync_weekly_avg_revenue_metrics,
    sync_transaction_cache,
    sync_weekly_data,
    sync_financials,
)

def main():
    print("=== Railway Cron Sync Started ===")

    # Step 1: Pull Activity -> transactions_cache (SWAP, SEND, CASH, DAPP)
    sync_transaction_cache()

    # Step 2: Extract swap fee data per chain
    sync_fee_series()

    # Step 3: Daily stats per chain (transactions, revenue, agents, referrals, etc.)
    sync_daily_stats()

    # Step 4: Weekly revenue averages
    sync_weekly_avg_revenue_metrics()

    # Step 5: Weekly data (cash volume, swap quantity, referrals, agents, etc.)
    sync_weekly_data()

    print("All sync jobs completed.")

if __name__ == "__main__":
    main()
