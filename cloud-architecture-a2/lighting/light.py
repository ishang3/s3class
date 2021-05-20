import json 
import math
import datetime 
from datetime import  timedelta
from sqlops.sendmessage import MessageSending
import threading
import sched, time



"""
Send data to activity table accordingly 
Different states of light: solid green, flashing green, yellow, red }

Flashing 
Green(10),None(10),Green(10),None(10),Green(10),

if the number of fluctuation is above a certain therehold - will consider the light flashing for that label

"""

class LightProcessing(MessageSending):
    def __init__(self,data,table_name,presence_table):
        MessageSending.__init__(self,table_name,presence_table)
        self.bool = False
        self.lights = {} #mapping between light id and [hysteresis,type,timestamp]
        self.hysteresis = 100 # CHANGEABLE; number of state before i declate it as that state

        self.buffer = {}
        self.sent = []
        
        #checkpoint code
        self.checkpoint_interval = 60
        f_stop = threading.Event()
        self.f(f_stop)

    def f(self,f_stop):     
        #make copy of the buffer
        buffercopy = self.buffer.copy()

        retvalues = {}
        
        #preprocess buffer
        for value in buffercopy:
            #go through each minute
            maxcolorcount = 0
            maxcolor = []
            #go through each color in minute
            for color in buffercopy[value]:
                colorval = buffercopy[value][color]
                if colorval > 0:
                    maxcolor.append(color)
                    maxcolorcount = colorval

            #need to fix to join ''.join()
            retvalues[value] = ''.join(maxcolor)
            
        for value in retvalues:
            #for this minute, it achieved higher or not then the min threshold 
            if value not in self.sent:
                self.sent.append(value)
                starting,ending = self.revert_timestamp(value)
                color = retvalues[value]
                print('sending data FOR LIGHT',starting,ending,color)
                self.send_to_db(starting,ending,color)
        
        #always remove key regardless from self.buffer
        self.buffer = {}
        retvalues = {}
        
        if not f_stop.is_set():
            # call f() again in 10 seconds
            #print('running the reporting engine *****************')
            threading.Timer(self.checkpoint_interval, self.f, [f_stop]).start()


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
        localtime = self.ntp_to_system_time(timestamp)
        local = datetime.datetime.fromtimestamp(localtime/1000000000)
        #local = local - timedelta(hours=12)
        year =  str(local.year)
        month = str(local.month)
        day   = str(local.day)
        hour  = str(local.hour)
        minute = str(local.minute)

        localstring = year + '.' + month + '.' + day + '.' + hour + '.' + minute
        return localstring

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


    def send_to_db(self,starting,ending,state):

        """
        Sending data to db 
        """
        #update the last change for this id

       
        ret = {
                'starting' : starting, 
                'ending'   : ending, #this is the current timestamp of when message was seen
                'type'     : 'light',
                'trackid'  : None,
                'day' :      int(ending.today().weekday()),
                'timeshort' : starting.strftime("%H:%M:%S"),
                'objectvalue' : state
            }

        #sending data to presence table
        self.send_objectid(ret)
        

    
    def parse_data(self,data):
        """
        """
        for val in data:
            lightvalue = data[val]
            splitted = lightvalue.split(':')
            color = splitted[0]
            colorvalue = splitted[1]

            #whatever light is on; need to fix this 
            if colorvalue == 'On':
                return color

        return None



    def process_light_data(self,data,timestamp):
        """
        process light data coming in
        [1:green,2:red,3:orange]
        """
        convertedtimestamp = self.fix_timestamp(int(timestamp))
        color = self.parse_data(data) #returns value which is turned on 

       

        if color is None:
            return

        if convertedtimestamp not in self.buffer:
            self.buffer[convertedtimestamp] = {color: 1}
        elif color not in self.buffer[convertedtimestamp]:
            self.buffer[convertedtimestamp][color] = 1
        else:
            self.buffer[convertedtimestamp][color] += 1

        



       



                







            













