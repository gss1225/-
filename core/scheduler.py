from apscheduler.schedulers.background import BackgroundScheduler
import httpx

from core.logger import get_scheduler_logger
logger = get_scheduler_logger()

def trigger_update_today():
    try:
        response = httpx.get('http://127.0.0.1:8000/update_today')
        if response.status_code != 200:
            logger.error(f"주식 갱신 중 오류")
        logger.info(f'주식 정보 갱신: {response.json().get("date")}')
    except Exception as e:
        logger.error(f"주식 갱신 중 오류: {e}")

scheduler = BackgroundScheduler()
scheduler.add_job(trigger_update_today, 'cron', hour=7, minute=0)

def start_scheduler():
    logger.info('Starting scheduler')
    scheduler.start()

def end_scheduler():
    logger.info('Stopping scheduler')
    scheduler.shutdown()