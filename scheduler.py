from apscheduler.schedulers.blocking import BlockingScheduler
from pipeline import run_pipeline
import logging

logging.basicConfig(level=logging.INFO)

scheduler = BlockingScheduler()

# Schedule pipeline to run every day at 8:00 AM
@scheduler.scheduled_job("cron", hour=8, minute=0)
def scheduled_job():
    print("⏰ Scheduler triggered — running pipeline...")
    run_pipeline()

print("🕐 Scheduler started — pipeline will run daily at 8:00 AM")
print("   Press Ctrl+C to stop")
scheduler.start()