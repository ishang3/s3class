import schedule
import time
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, inspect, TIMESTAMP,Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence
import datetime

class CronJob:
    def __init__(self,):
        self.mapping = {}
        self.company_name = 'alvaprod'#'ulvaprod'
        self.company_id = ['58','60']#['15','16']
        DATABASE_URI = 'postgresql://postgres:sensable01@sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com:5432/demo'
        self.engine = create_engine(DATABASE_URI)
        self.conn = psycopg2.connect(
			host='sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com',
			port=5432,
			user='postgres',
			password='sensable01',
			database='demo'
			)
        self.cur = self.conn.cursor()
        #self.check_table_names()
        self.create_schedule()
        self.run_job()

    
    def create_insert_statement(self,lower,upper,day,presence_table_name,shift_table_name):


        # datetime object containing current date and time
        now = datetime.datetime.now()
        # dd/mm/YY H:M:S
        #dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
        dt_string = now.strftime("%Y-%m-%d")
        nowstarting = dt_string + ' ' + lower
        nowending = dt_string + ' ' + upper
        insert_str = """
        INSERT into {shift_table_name} (type, objectvalue, count, load,starting)
        select  type, objectvalue, count(type),sum(ending-starting) as load, '{now}' as starting  from {presence_table_name}
        where starting >= '{lower}' and ending < '{upper}' and day = {day}
        group by type, day, objectvalue
                    """.format(shift_table_name=shift_table_name,
                               presence_table_name=presence_table_name,
                               lower=nowstarting,
                               upper=nowending,
                               day = day,
                               now = nowstarting, #this is the starting of the 10 minute entry
                    )

        return insert_str

    def check_table_names(self,):
        ins = inspect(self.engine).get_table_names()
        ret = []
        for x in ins:
            total = len(x.split('_'))

            #checks if the company id is in the ones we are looking for
            #if not then continue
           
            if total == 3 and x.split('_')[0] == self.company_name and x.split('_')[-1] == 'presence' and x.split('_')[1] in self.company_id:
                ret.append(x)
    
        return ret

    def create_job(self,job_time,day,lower,upper):
        #here i need to iterate through all the availible tables with the presence; then go about it accordingly 

        #this will return the table names for presence as well as the ones for the company name 
        for p_table in self.check_table_names():
            table_list = p_table.split('_')
            table_list.remove('presence')
            table_list.append('shift')
            shift_table_name = '_'.join(table_list)
            

            insert_stmt = self.create_insert_statement(lower,upper,day,p_table,shift_table_name)
            
            
            #check for weekday which set today
            weekday = datetime.datetime.today().weekday()
            if weekday == day:
                print(shift_table_name,'*****************************')
                print('*****************************************')
                print(insert_stmt)
                print('*****************************************')
                self.cur.execute(insert_stmt)
                self.conn.commit()
           


    def create_schedule(self,):

        for day in range(7): #this value should be 7
            for hour in range(24): #this value should be 24
                for minute in range(0,60,10): #this value should be 0,60,10
                    lower, upper,cron_higher = self.create_time_bounds(hour,minute)
                    if cron_higher == '24:01' or cron_higher == '24:09':
                        continue

                    self.mapping[(cron_higher,day)] = (lower,upper)

        
        
        for x in self.mapping:
            job_time, day = x
            lower,upper = self.mapping[x]
            #print(x,self.mapping[x])
            #self.create_job(job_time,day,lower,upper)
            #print(job_time,'***************')

            #print(day)
            schedule.every().day.at(job_time).do(self.create_job,job_time,day,lower,upper)

            continue
            
            # if day == 2:
            #     print(day)
            #     schedule.every().day.at(job_time).do(self.create_job,job_time,day,lower,upper)
            
    
    def run_job(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

    def create_time_bounds(self,hour,minute):

        """
        This creates bounds of the timing intervals
        """

        lower = f'{hour}:{minute:02}:00'
        if minute + 10 == 60:
            higher = f'{hour+1}:{00}:00'
            cron_higher = f'{hour+1:02}:01'
        else:
            higher = f'{hour}:{minute+10}:00'
            cron_higher = f'{hour:02}:{minute+10+9}'
        return lower,higher,cron_higher
    

