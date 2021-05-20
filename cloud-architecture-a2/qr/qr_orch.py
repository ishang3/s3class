import json 
import math
from sqlops.sendmessage import MessageSending
from datetime import  timedelta
import datetime
import threading
import sched, time

"""
Send data to presence table accordingly 
Need to check if it complies with the other presence code written
line Annotation tool thing checkup
"""

class QrProcessing(MessageSending):
    def __init__(self,data,table_name,presence_table):
        MessageSending.__init__(self,table_name,presence_table)
        self.buffer = {}
        self.value = []
        self.sent = []

        #checkpoint code
        self.checkpoint_interval = 60
        f_stop = threading.Event()
        self.f(f_stop)

    def f(self,f_stop):



        buffercopy = self.buffer.copy()

        retvalues = {}
        
        #preprocess buffer
        for value in buffercopy:
            #go through each minute

            if value not in self.sent:
                self.sent.append(value)
                starting,ending = self.revert_timestamp(value)
                qrvalue = buffercopy[value]
                print('sending data FOR QR',starting,ending,qrvalue)
                self.send_to_db(starting,ending,qrvalue)
        
        #always remove key regardless from self.buffer
        self.buffer = {}
        
        if not f_stop.is_set():
            # call f() again in 10 seconds
            #print('running the reporting engine *****************')
            threading.Timer(self.checkpoint_interval, self.f, [f_stop]).start()

    def send_to_db(self,starting,ending,state):

        """
        Sending data to db 
        """
        #update the last change for this id

       
        ret = {
                'starting' : starting, 
                'ending'   : ending, #this is the current timestamp of when message was seen
                'type'     : 'qr',
                'trackid'  : None,
                'day' :      int(ending.today().weekday()),
                'timeshort' : ending.strftime("%H:%M:%S"),
                'objectvalue' : state
            }

        #sending data to presence table
        self.send_objectid(ret)
    

    
    def revert_timestamp(self,timestamp):

        #create 2 timestamps, timestamp; timestamp += 1
        splitted = timestamp.split('.')

        year = splitted[0]
        month = splitted[1]
        day = splitted[2]
        hour = splitted[3]
        minute = splitted[4]

        dt = datetime.datetime(int(year),int(month),int(day))
        t = datetime.time(int(hour), int(minute))
        starting = datetime.datetime.combine(dt.date(), t)

        if int(minute) == 59:
            t_end = datetime.time(int(hour)+1, 0)
            ending = datetime.datetime.combine(dt.date(), t_end)
            return starting,ending

        t_end = datetime.time(int(hour), int(minute) + 1)
        ending = datetime.datetime.combine(dt.date(), t_end)

        return starting,ending

    def ntp_to_system_time(self,date):
        """convert a NTP time to system time"""
        SYSTEM_EPOCH = datetime.date(*time.gmtime(0)[0:3])
        NTP_EPOCH = datetime.date(1900, 1, 1)
        NTP_DELTA = (SYSTEM_EPOCH - NTP_EPOCH).days * 24 * 3600
        return date - NTP_DELTA

    def fix_timestamp(self,timestamp):
        """
        this formats the timestamp sent by muvva to the correct python format
        """

        ret = [] 

        localtime = self.ntp_to_system_time(timestamp)
        local = datetime.datetime.fromtimestamp(localtime/1000000000)
        #local = local - timedelta(hours=12)
        year =  str(local.year)
        month = str(local.month)
        day   = str(local.day)
        hour  = str(local.hour)
        minute = str(local.minute)


        localstring = year + '.' + month + '.' + day + '.' + hour + '.' + minute
        ret.append(localstring)

        for x in range(1,11):
            newminute = int(minute)
            newminute += x

            if newminute >= 60:
                continue

            newlocalstring = year + '.' + month + '.' + day + '.' + hour + '.' + str(newminute)
            ret.append(newlocalstring)

        return ret
       
       #1 -> 100 frames empty -> 1
    def process_input_data(self,data,timestamp,topic):
        """
        data: expect this to be only the labels 
        This will take in the data coming from the iot core
        data: [1,2,3] list of labels 
        timestamp: value of time


        Every time a state has been constant for 100 consecutive frames
        then send to the database
        """
        # print(data)

        #ignore the data, it is nothing is being sent
        if data == ['Hold']:
            return 

        #print(self.previous_data,'!!!!!!!!!!!!!!!!!!!')
        #check if data contains 0 number of elements
        #then increment each value of the hashmap by owne
        if  data == ['Empty']:
            return 

        qrvalue = data[0]

        convertedtimestamp = self.fix_timestamp(int(timestamp))

        for val in convertedtimestamp:
            if val not in self.buffer:
                self.buffer[val] = qrvalue 
            

        
        
        


        
            










