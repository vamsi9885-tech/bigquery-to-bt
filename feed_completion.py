import logging
import time
import datetime
import azure.functions as func
import os, sys
module_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(module_dir)
from fc import FeedCompletion
from Utils.logger_utils import configure_root_logger

def main(Functimer: func.TimerRequest, context:func.Context) -> None:
    configure_root_logger(context)
    logging.info('Send_Feed_Completion_Notification function called.')
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    if Functimer.past_due:
        logging.info('The timer is past due!')

    try:
        fc_obj = FeedCompletion()
        fc_obj.trigger_feed_completion_function_app()
        time.sleep(3)
        logging.info(f"Feed Completion Notification timer trigger function triggered at", extra={"properties":{'timestamp': utc_timestamp}})
    except Exception:
        logging.error("Failed to trigger the second function",exc_info=True)
