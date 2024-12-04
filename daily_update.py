import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
import time
from k_stockinfo import get_db_connection, get_stock_codes, get_k_data, insert_k_data
from config import BAOSTOCK_CONFIG
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='daily_update.log'
)

def update_daily_kline():
    try:
        # Login to baostock
        bs.login()
        
        # Get yesterday's date
        today = datetime.now()
        yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logging.info(f"Starting daily update for date: {yesterday}")
        
        # Get all stock codes
        stock_codes = get_stock_codes()
        total_stocks = len(stock_codes)
        
        for idx, code in enumerate(stock_codes, 1):
            try:
                # Get K-line data for yesterday
                k_data = get_k_data(code, yesterday, yesterday)
                
                if not k_data.empty:
                    # Insert data into database
                    insert_k_data(k_data)
                    logging.info(f"Successfully updated {code} ({idx}/{total_stocks})")
                else:
                    logging.warning(f"No data available for {code} on {yesterday}")
                
                # Add delay between API calls
                time.sleep(BAOSTOCK_CONFIG['delay_seconds'])
                
            except Exception as e:
                logging.error(f"Error processing stock {code}: {str(e)}")
                continue
        
        logging.info("Daily update completed successfully")
        
    except Exception as e:
        logging.error(f"Error in daily update: {str(e)}")
    
    finally:
        # Logout from baostock
        bs.logout()

if __name__ == "__main__":
    update_daily_kline()
