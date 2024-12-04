from apscheduler.schedulers.blocking import BlockingScheduler
from daily_update import update_daily_kline
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='scheduler.log'
)

def start_scheduler():
    scheduler = BlockingScheduler()
    
    # Schedule the job to run at 20:00 (8 PM) every day
    scheduler.add_job(
        update_daily_kline,
        trigger='cron',
        hour=20,
        minute=0,
        name='daily_kline_update'
    )
    
    logging.info("Scheduler started")
    scheduler.start()

if __name__ == "__main__":
    try:
        start_scheduler()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped")
