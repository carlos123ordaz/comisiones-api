from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime

scheduler = BackgroundScheduler()
last_auto_sync = {"timestamp": None, "status": None, "message": None}


def sync_bitrix_job():
    """Job que se ejecuta automaticamente todos los dias a las 8:00 AM."""
    from services.bitrix_service import fetch_invoices_from_bitrix
    from services.report_service import execute_report
    from config.database import db

    print(f"[CRON] Iniciando sync automatico con Bitrix24 - {datetime.now()}")
    try:
        df_invoices = fetch_invoices_from_bitrix()
        execute_report(data_invoices=df_invoices, data_ventas=None)
        last_auto_sync["timestamp"] = datetime.now().isoformat()
        last_auto_sync["status"] = "success"
        last_auto_sync["message"] = f"{len(df_invoices)} invoices sincronizadas"
        db["sync_log"].insert_one({
            "timestamp": last_auto_sync["timestamp"],
            "type": "auto",
            "status": "success",
            "message": last_auto_sync["message"],
        })
        print(f"[CRON] Sync exitoso: {last_auto_sync['message']}")
    except Exception as e:
        last_auto_sync["timestamp"] = datetime.now().isoformat()
        last_auto_sync["status"] = "error"
        last_auto_sync["message"] = str(e)
        db["sync_log"].insert_one({
            "timestamp": last_auto_sync["timestamp"],
            "type": "auto",
            "status": "error",
            "message": last_auto_sync["message"],
        })
        print(f"[CRON] Error en sync: {e}")


def start_scheduler():
    scheduler.add_job(
        sync_bitrix_job,
        trigger=CronTrigger(hour=8, minute=0),
        id="daily_bitrix_sync",
        replace_existing=True,
    )
    scheduler.start()
    print("[CRON] Scheduler iniciado - sync diario programado a las 8:00 AM")
