#!/usr/bin/env python3

import warnings
import time
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from data.extraction_scripts.IND_stockprice_extraction import IND_stockprice_extraction
from data.extraction_scripts.IND_dailydata_extraction import IND_dailydata_extraction
from data.extraction_scripts.US_stockprice_extraction import US_stockprice_extraction
from data.extraction_scripts.US_dailydata_extraction import US_dailydata_extraction

warnings.filterwarnings("ignore")

scheduler = BlockingScheduler()
scheduler.add_job(IND_stockprice_extraction, 'cron', day_of_week='mon-fri',
                  hour='9-15', minute='*/1', timezone=pytz.timezone('Asia/Kolkata'))
scheduler.add_job(IND_dailydata_extraction, 'cron',
                  day_of_week='mon-fri', hour='9', minute='1', timezone=pytz.timezone('Asia/Kolkata'))
scheduler.add_job(US_stockprice_extraction, 'cron',
                  day_of_week='mon-fri', hour='9-16', minute='*/1', timezone=pytz.timezone('America/New_York'))
scheduler.add_job(US_dailydata_extraction, 'cron',
                  day_of_week='mon-fri', hour='9', minute='1', timezone=pytz.timezone('America/New_York'))

while 1:
    scheduler.start()
    time.sleep(1)
