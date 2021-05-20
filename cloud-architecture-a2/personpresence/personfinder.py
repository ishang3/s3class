import json 
import math
import datetime 
from datetime import  timedelta
from sqlops.sendmessage import MessageSending
import threading
import sched, time
from datetime import time
import cloudconfig



"""
"""

class PersonFinder(MessageSending):
    def __init__(self,data,table_name,presence_table):
        MessageSending.__init__(self,table_name,presence_table)

        self.data = data
        self.clip_input = True

        #this will create a buffer for the past some minutes to capture the state 
        self.buffer = {}
        self.sent = []
        self.frame_threshold = 100
        self.delta = 1/cloudconfig.infer_fps
      
        #checkpoint code
        self.checkpoint_interval = 300
        f_stop = threading.Event()
        self.f(f_stop)

    def fix_frameid_timestamp(self,seconds):
        local = datetime.datetime.now()
        year =  str(local.year)
        month = str(local.month)
        day   = str(local.day)
        hour  = str(local.hour)
        minute = str(int(seconds//60))
        localstring = year + '.' + month + '.' + day + '.' + hour + '.' + minute
        return localstring

        pass

    def fix_iot_timestamp(self, timestamp):
        converted = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        local = converted- timedelta(hours=7)
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



# Buffer 1:30 -> 1:35 is sent at approximately 1:35 PM
# 


    def f(self,f_stop):
    # do something here ...
        current_time = datetime.datetime.now()
        
        #make copy of the buffer
        buffercopy = self.buffer.copy()
        for value in buffercopy:
            #value -> value + 1
            active_frames = self.buffer[value][0]
            active_state = self.buffer[value][1]

            #for this minute, it achieved higher or not then the min threshold 
            if active_frames > self.frame_threshold and value not in self.sent:
                self.sent.append(value)
                starting,ending = self.revert_timestamp(value)
                print('sending data FOR PERSON',starting,ending,active_state)
                self.send_to_db(starting,ending,active_state)

        #always remove key regardless from self.buffer
        self.buffer = {}
        
        if not f_stop.is_set():
            # call f() again in 10 seconds
            #print('running the reporting engine *****************')
            threading.Timer(self.checkpoint_interval, self.f, [f_stop]).start()
    
    def process_frame_state(self,framestate,frameid):
        """
        process the frame state to understand when an id has left a certain region

        First time seeing state, then create a mappings

        if an id does not appear in the same state agin in a future frame, then
        it is over for that id - then send to db
        """
       
        #reset for every frame state
        # print('******')
        # print(self.data)
        for id in framestate:
            current_state = framestate[id]['current_state']
            object_id = framestate[id]['object_id']
            object_type = framestate[id]['object_type']
            timestamp = framestate[id]['frame_time']
            if type(object_type) == type('test'):
                object_type = object_type.lower()
           # print(object_type,current_state,self.data)

    
            if len(current_state) == 0:
                continue

             #first check if object type exists
            if object_type not in self.data:
                continue

            #then check if state exists for that object type
            if current_state[0] not in self.data[object_type]:
                continue

            if self.clip_input == True:
                #get deltatime by the frameid
                deltatime = self.delta * frameid
                convertedtimestamp = self.fix_frameid_timestamp(deltatime)

            if self.clip_input == False:
                convertedtimestamp = self.fix_iot_timestamp(timestamp)
                

            if convertedtimestamp not in self.buffer:
                self.buffer[convertedtimestamp] = [1,current_state[0]]
            else:
                self.buffer[convertedtimestamp][0] += 1 

            #print(self.buffer)

           



    
    def send_to_db(self,starting,ending,state):

        """
        Sending data to db 
        """
        #update the last change for this id

       
        ret = {
                'starting' : starting, 
                'ending'   : ending, #this is the current timestamp of when message was seen
                'type'     : 'regionpresence',
                'trackid'  : None,
                'day' :      int(ending.today().weekday()),
                'timeshort' : starting.strftime("%H:%M:%S"),
                'objectvalue' : state
            }

        #sending data to presence table
        self.send_objectid(ret)
        self.previous_minute_state = False



#410 - Present
