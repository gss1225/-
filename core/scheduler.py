from apscheduler.schedulers.background import BackgroundScheduler
import httpx
import os

from core.logger import get_scheduler_logger
logger = get_scheduler_logger()

def send_discord_webhook(content):
    try:
        url = os.getenv("WEBHOOK_URL")
        if not url:  # ignore
            return
        payload = {"content": content}
        response = httpx.post(url, json=payload)
        if response.status_code == 204:
            return
        else:
            logger.error(f"Discord webhook failed: {response.status_code} {response.text}")
    except Exception as e:
        logger.error(f"Discord webhook error: {e}")

def trigger_update_today():
    try:
        response = httpx.get('http://127.0.0.1:8000/update_today')
        if response.status_code != 200:
            send_discord_webhook(f"주식 갱신 중 오류")
            logger.error(f"주식 갱신 중 오류")
        send_discord_webhook(f'주식 정보 갱신: {response.json().get("date")}')
        logger.info(f'주식 정보 갱신: {response.json().get("date")}')
    except Exception as e:
        send_discord_webhook(f"주식 갱신 중 오류: {e}")
        logger.error(f"주식 갱신 중 오류: {e}")

scheduler = BackgroundScheduler()
scheduler.add_job(trigger_update_today, 'cron', hour=7, minute=0)

def start_scheduler():
    logger.info('Starting scheduler')
    scheduler.start()

def end_scheduler():
    logger.info('Stopping scheduler')
    scheduler.shutdown()

'''
주식 주문 방법
1. get total assets
2. get portfolio weights -> need a way to select the weights to use  (db?)
3. find assets per stock needed in KRW => save expected amount and send to discord + db 
4. buy or sell as needed  -> send total order data.
    4.1 current price data is in account stock info
    4.2 figure out closest viable order
5. check periodically for changes to order status => send reports
'''