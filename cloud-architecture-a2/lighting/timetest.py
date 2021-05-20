
import datetime
import time
from datetime import  timedelta

SYSTEM_EPOCH = datetime.date(*time.gmtime(0)[0:3])
NTP_EPOCH = datetime.date(1900, 1, 1)
NTP_DELTA = (SYSTEM_EPOCH - NTP_EPOCH).days * 24 * 3600
def ntp_to_system_time(date):
    """convert a NTP time to system time"""
    return date - NTP_DELTA



local_time = ntp_to_system_time(1617216046934943000)

local = datetime.datetime.fromtimestamp(local_time/1000000000)
#local = local - timedelta(hours=12)
year =  str(local.year)
month = str(local.month)
day   = str(local.day)
hour  = str(local.hour)
minute = str(local.minute)

print(year,month,day,hour,minute)