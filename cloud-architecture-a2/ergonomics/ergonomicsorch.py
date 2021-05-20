# std libs
import json
import math
from sqlops.sendmessage import MessageSending
import datetime

# local libs
import cloudconfig


class Ergonomics(MessageSending):
    def __init__(self,data,table_name,presence_table):
        MessageSending.__init__(self,table_name,presence_table)
        self.window_size = 30
        self.values = []
        self.sum = 0
        self.rules = data # maps from threeKP (tuple) to [angleCompare,angleThresh,tmin]
        self.delta = 1/cloudconfig.infer_fps
        self.counter = 0     
        self.checkpoint_history = {} #checkpoint
        self.last_flush_frame = 0
        self.hysteresis_length = cloudconfig.ergo_hysteresis
        self.frame_count = 0

        self.object_type_mappings = {
            'pallet' : 'pl',
            'forklift' : 'fl',
            'Person'   : 'p',
             0     : 'pa',
             1     : 'pb',
             2     : 'pc',
             3     : 'pd',
             4     : 'pe',
             5     : 'pf',
             6     : 'pg',
             7     : 'ph',
             8     : 'pi',
             9     : 'pj',
             10     : 'pk',
             11     : 'pl',
             12     : 'pm',
             13     : 'pn',
             14     : 'po',
             15     : 'pp',
             16     : 'pq',
             17     : 'pr'
        }


    def getstartminmax(self,dict):
        """
        input: Dictionary 
        output: 2 timestamps, respective sources

        take the min of the keys (sources) then take the min of that key
        take the max of the keys (sources)  then take the max of that key

        starting source will always be the min key

        object id:  region : GL260037_c2.mp4 : [frametime,frametime]
        GL270037_c2.mp4  :  [frametime,frametime]
        GL280037_c2.mp4   :  [frametime,frametime]
        """
        #get the smallest key
        starting_source = min(dict.keys())
        #take the min of the smallest key
        starting_value = min(dict[starting_source])

        #get the largest value
        ending_source  = max(dict.keys())
        ending_value =   max(dict[ending_source])

        return starting_source,starting_value,ending_source,ending_value


    def send_data(self,object_id,threeKP,starting,ending,startSource, endSource,relativestarting,relativending):
        if(cloudconfig.multiple_json_flag==True):
            totaltime = relativending - relativestarting
        else:
            totaltime = ending - starting
        # filter out with tmin
        if totaltime < self.rules[threeKP][2]:
            return
        
        print('SENDING ERGO DATA TO DB !!!!!')
        payload = {
                'startime' : '2021-03-23 ' + str(datetime.timedelta(seconds=starting)),
                'endtime' :   '2021-03-23 ' + str(datetime.timedelta(seconds=ending)),
                'totaltime' : totaltime,
                'trackid'  : object_id,
                'match'    : str(threeKP) + str(self.rules[threeKP]), # was threeKP,
                'relativestarting' : '2021-03-23 ' + str(datetime.timedelta(seconds=relativestarting)),
                'relativending' : '2021-03-23 ' + str(datetime.timedelta(seconds=relativending)),
                'startingsource': startSource,
                'endingsource' :   endSource
                 }
        
        print(payload,'THIS IS THE DATA THAT IS BEING SENT TO THE DB FOR ERGO')
        self.send_match(payload)
        print('sent ergo data to db')
        return


    def calculate_vertical_line(self,):
        pass

    # based on similar function from posepresence.py
    def reformat_framestate(self,framestate):
        """
        Reformat framestate
        
        orig framestate[str(objectid)] = { #Number + kp 
                        'object_type' : mappings[objectid],
                        'object_id'   : objectid,
                        'centroid'    : centroid, # coord of kp
                        'current_state' : currentState,
                        'frame_time' :   timestamp #datetime.datetime.now() #date_time_obj = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')
            }


        new framestate mapping: trackid ("1") -> kp name (pa to pr) -> tuple (x,y) of kp
        """
        reformatted_dict = {}
        for objId in framestate:
            kpinfo = framestate[objId] # info for this kp in this frame
            trackid =  ''.join(list(kpinfo['object_id'])[:-1]) #7c -> 7          
            keypoint_object_type = self.object_type_mappings[kpinfo['object_type']] # output eg: "pa"

            # build reformatted dict 
            if trackid not in reformatted_dict:
                reformatted_dict[trackid] = {keypoint_object_type: kpinfo['centroid']} # trackid -> "pa" -> coord
                continue
            #first time seeing threeKP in trackid in reformatted dict
            if keypoint_object_type not in reformatted_dict[trackid]:
                reformatted_dict[trackid][keypoint_object_type] = kpinfo['centroid']
                continue

            #already have seen trackid and object type
            reformatted_dict[trackid][keypoint_object_type] = kpinfo['centroid']
            
        return reformatted_dict


    def flush(self,currframeid):
        """
        This function runs once every {x} number of frames
        """
        #iterate through checkpoint history
        #go through each object id
        remove = []
        for object_id in self.checkpoint_history:
            #go through each threeKP tuple for each object id
            if len(self.checkpoint_history[object_id]) == 0:
                remove.append((object_id,None))
            for threeKP in self.checkpoint_history[object_id]:
                #then get the list of frameids appended in this log for this region
                lastframeid = self.checkpoint_history[object_id][threeKP][0][-1] #this gets the last frameid
                #lastframeid -> 1,R7,100
                #curr frameid -> 600
                difference = currframeid - lastframeid
                if difference > self.hysteresis_length:
                    #send to db
                    #extract min,max and sources from object_region_time
                    startingsource, starting, endingsource,ending = self.getstartminmax(self.checkpoint_history[object_id][threeKP][1])
                    relativestarting = min(self.checkpoint_history[object_id][threeKP][3])
                    relativending = max(self.checkpoint_history[object_id][threeKP][3])
                    self.send_data(object_id,threeKP,starting,ending,startingsource,endingsource,relativestarting,relativending)

                    #now remove only the threeKP tuple from the respective id from checkpoint history
                    #object id - > threeKP - > [[frameid], [frametimes]
                    remove.append((object_id,threeKP))

        #removing outdated values in checkpoint history
        for val in remove:
            objectid,threeKP = val
            #Region is none means remove objectid
            if threeKP is None:
                self.checkpoint_history.pop(objectid,None)
            else:
                self.checkpoint_history[objectid].pop(threeKP,None)

        return


    def process_frame_state(self,framestate,frameid,source=None,relativeframeid=None):
        """
        this is taking the keypoints data straight from the iot core data
        INPUTS:
            framestate[str(objectid)] = { #Number + kp 
                        'object_type' : mappings[objectid],
                        'object_id'   : objectid,
                        'centroid'    : centroid, # coord of kp
                        'current_state' : currentState,
                        'frame_time' :   timestamp #datetime.datetime.now() #date_time_obj = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')
            }
            frameid: current frameid
        """    

        '''
        for each object id in framestate: # object id eg : 1a, 1b, 4c, 11b etc
        '''
        frame_time = self.delta * frameid
        if(cloudconfig.multiple_json_flag==True):
            relative_frame_time = self.delta * relativeframeid
            checkpoint_frame_id = relativeframeid
        else:
            relative_frame_time = frame_time
            checkpiont_frame_id = frame_id

        # debug prints
        # print(frame_time)

        """
        #FLUSH IF NEEDED
        #if frametime - lastflushtime > 10 seconds OR frameid - flushid > 500 
        #START FLUSHING THE CHECKPOINT STRUCTURE; arbitrary number 
  
        reformat framestate: map(trackid -> region) -> keypoints : NEW
        Iterating over framestate:
        For each trackid in framestate
            for each rule
                check if the set of keypoints for this id includes the 3Kps in the UI rule and that they conform to the ergo rule
                If not continue
                If yes:
                    add it to the checkpoint structure if it's not there: {WHOLE THING: .... self.checkpoint_history[object_id] = [[frameid], [frame_time],{object_type}]}
                    if its there (means id: this 3KP tuple exist)
                        then check the last frameid . If it's within the hysteresis then just add this and update the frameid, frametime...
                        if not within the hysteresis then this person has gone and come back in so expire the previous one and start a new timer

        """

        self.frame_count += 1

        #handle flushing code
        if checkpoint_frame_id > (self.last_flush_frame + 500):
            print("Flushing ergo data!")
            self.flush(checkpoint_frame_id)
            self.last_flush_frame = checkpoint_frame_id
       
        #uncomment for sending framestate for simulation
        # self.send_frame_state_todb(framestate,frameid)
        # return
       
        # print("#############")
        # print("orig framesate")
        # print(framestate)
        # print("#############")

        framestate = self.reformat_framestate(framestate)

        # print('reformatted framestate')
        # print(framestate)
        # print("#############")

        for object_id in framestate:
            for threeKP in self.rules:
                skipRule=False
                for kp in threeKP:
                    if kp not in framestate[object_id]:
                        skipRule=True # go to the next rule

                if(skipRule):
                    continue

                # here we have that all 3 kps in the rule are present in the current frame
                # check if those 3 kps conform to this ergo rule
                coords = []
                for kp in threeKP:
                    coords.append(framestate[object_id][kp])

                #if angle is greater 180; then do 360 - that angle
                #if angle is less then or equal 180 then that is the angle i am looking at        
                angle = self.get_angle(tuple(coords))
                if(angle>180):
                    angle = 360-angle

                angleCompare = self.rules[threeKP][0] # eg ">"
                angleThresh = self.rules[threeKP][1] # eg 70
                tmin = self.rules[threeKP][2] # eg 0.5

                if (angleCompare=='>' and angle > angleThresh) or (angleCompare=='<' and angle<angleThresh):
                    #check if id exists 
                    if object_id not in self.checkpoint_history:
                        self.checkpoint_history[object_id] = {threeKP: [[checkpoint_frame_id], {source:[frame_time]},source,[relativeframetime]]}
                        continue

                    if threeKP not in self.checkpoint_history[object_id]:
                        self.checkpoint_history[object_id][threeKP] = [ [checkpoint_frame_id], {source:[frame_time]},source,[relativeframetime]]
                        continue

                    #check for source in dictionary
                    if source not in self.checkpoint_history[object_id][threeKP][1]:
                        self.checkpoint_history[object_id][region][1][source] = [frame_time]

                    #if it does exist then compare frame ids
                    lastframeid = self.checkpoint_history[object_id][threeKP][0][-1]
                    if(checkpoint_frame_id==lastframeid):
                        continue

                    hysteresis_bool = checkpoint_frame_id < lastframeid + self.hysteresis_length
                    if hysteresis_bool:
                        #update last frameid and frametime
                        self.checkpoint_history[object_id][threeKP][0].append(checkpoint_frame_id)
                        self.checkpoint_history[object_id][threeKP][1][source].append(frame_time)
                        self.checkpoint_history[object_id][threeKP][3].append(relativeframetime)
                    else:
                        # send to db
                        print("sending ergo data to db...")
                        #extract min,max and sources from object_region_time
                        startingsource, starting, endingsource,ending = self.getstartminmax(self.checkpoint_history[object_id][threeKP][1])
                        relativestarting = min(self.checkpoint_history[object_id][threeKP][3])
                        relativending= max(self.checkpoint_history[object_id][threeKP][3])
                        self.send_data(object_id,threeKP,starting,ending,startingsource,endingsource,relativestarting,relativending)

                        #reset checkpoint history
                        self.checkpoint_history[object_id][threeKP][1] = {source:[frame_time]} #reset frame time
                        self.checkpoint_history[object_id][threeKP][0] = [checkpoint_frame_id]     #reset frameid
                        self.checkpoint_history[object_id][threeKP][3] = [relativeframetime]     #reset relative frame time

            # debug prints
            #print('***********************************')
            #print('***********************************')
            return 

        ''' OLD CODE

        object_id = 1

        frame_time = self.delta * frameid

        # if there are no keypoints in current frame, then exit
        if len(kp) == 0:
            return 

        for kp_set in kp:
            temp = {}
            for val in self.threeKP:
                if val not in kp_set:
                    break
                else:
                    temp[val] = kp_set[val]        
            coords = []
            if len(temp) == len(self.threeKP):
                for key,val in temp.items():
                    x,y = val
                    coords.append((x,y))


                #if angle is greater 180; then do 360 - that angle
                #if angle is less then or equal 180 then that is the angle i am looking at
                
                angle = self.get_angle(tuple(coords)) # smaller angle
                other_angle = 360-angle

                # print(angle,other_angle,'**********',frame_time)
                # return
                #************
                if angle > 70 and angle < 105:
                    #print(angle,other_angle,'**********',frame_time)

                    #check if id exists 
                    if object_id not in self.checkpoint_history:
                        self.checkpoint_history[object_id] = [frameid, [frame_time]]
                        return

                    #if it does exist then compare frame ids
                    lastframeid = self.checkpoint_history[object_id][0]
                    if frameid == lastframeid + 1:
                        #update last frameid
                        self.checkpoint_history[object_id][0] = frameid
                        #also append frametime too
                        self.checkpoint_history[object_id][1].append(frame_time)
                        return
                    #send to db
                    else:
                        print('sending data ')
                        starting = min(self.checkpoint_history[object_id][1])
                        ending =  max(self.checkpoint_history[object_id][1])
                        self.send_data(object_id,'Bending',starting,ending)

                        #reset checkpoint history
                        self.checkpoint_history[object_id][1] = [frame_time]
                        self.checkpoint_history[object_id][0] = frameid
                        
                return
                #************
                if self.equality == '<' and other_angle < self.threshold:
                    print(other_angle,'!!!!!!!!!!!',self.equality,self.threshold)
                    self.send_data(frameid)
                if self.equality == '>' and other_angle > self.threshold:
                    print(other_angle,'***********')
                    self.send_data(frameid)
        '''         

           
    def getAngle(self,a, b, c):
        ang = math.degrees(math.atan2(c[1] - b[1], c[0] - b[0]) - math.atan2(a[1] - b[1], a[0] - b[0]))
        return ang + 360 if ang < 0 else ang

    def process(self, value):
        self.values.append(value)
        self.sum += value
        if len(self.values) > self.window_size:
            self.sum -= self.values.pop(0)
        return float(self.sum) / len(self.values)

    def get_angle(self,coords):
        a,b,c = coords
        return self.getAngle(a,b,c)




      

