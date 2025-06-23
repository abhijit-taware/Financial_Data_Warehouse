from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import pytz
import logging
import pandas_market_calendars as mcal

#configure logging
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

#Market Configurations

MARKET_CONFIGS={
    'NYSE':{
        'timezone':'America/New_York',
        'open_time':'09:30',
        'close_time':'16:00',
        'holidays_calendar':'NYSE'
    },
    
}

def check_market_status(**context):
    '''
        Check if markets are currently open or closed
        Return market status for all configured markets
    '''
    current_time=datetime.now()
    current_date=datetime.now().date()
    market_status={}

    for market,config in MARKET_CONFIGS.items():
        try:
            #Get market timezone
            market_tz=pytz.timezone(config['timezone'])
            market_time=current_time.astimezone(market_tz)

            #Check if it's a weekday (Monday=0,sunday=6)
            is_weekday=market_time.weekday()<5

            #Parse market hours
            open_time=datetime.strptime(config['open_time'],'%H:%M').time()
            close_time=datetime.strptime(config['close_time'],'%H:%M').time()

            #Check if current time is within market hours
            is_market_hour=open_time<=market_time.time()<=close_time

            #Check if its a holiday 
            calendar=mcal.get_calendar(config['holidays_calendar'])
            schedule=calendar.schedule(str(current_date),str(current_date))
            is_holiday=schedule.empty

            #Check if its a weekday and there is no holiday
            is_open =is_weekday and is_market_hour and not is_holiday

            market_status[market]={
                'is_open':is_open,
                'local_time':market_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'next_open':None,
                'next_close':None
            }

            logger.info(f"{market} Status: {'OPEN' if is_open else 'CLOSED'} at {market_time}")

        except Exception as e:
            logger.error(f"Error checking {market} status: {str(e)}")
            market_status[market]={'is_open': False,'error':str(e)}

    #store results in XCOM for downstream tasks
    context['task_instance'].xcom_push(key="market_status",value=market_status)
    #return market_status

check_market_task = PythonOperator(
        task_id='check_market_status',
        python_callable=check_market_status,
    )



'''
 'NASDAQ':{
        'timezone':'America/New_York',
        'open_time':'09:30',
        'close_time':'16:00',
        'holidays_calendar':'NYSE'
    },
     'LSE':{
        'timezone':'Europe/London',
        'open_time':'08:00',
        'close_time':'16:30',
        'holidays_calendar':'LSE'
    },
     'JPX':{
        'timezone':'Asia/Tokyo',
        'open_time':'09:30',
        'close_time':'16:00',
        'holidays_calendar':'JPX'
    },
'''