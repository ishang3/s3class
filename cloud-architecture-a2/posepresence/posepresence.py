import json 
import math
import datetime 
from datetime import  timedelta
from sqlops.sendmessage import MessageSending
from sqlops.sendtempose import TempSending
from staplesorch.helpers import midpoint
from staplesorch.helpers import calculate_distance
import threading
import sched, time
import cloudconfig

class PoseFinder(MessageSending,TempSending):
    def __init__(self,data,table_name,presence_table):
        MessageSending.__init__(self,table_name,presence_table)
        TempSending.__init__(self,)

        self.data = data # {pk: [R0,R1]} -> {R0: [pk,pj,pl]}
        self.delta = 1/cloudconfig.infer_fps
        self.count = 0
        self.idle_orch = False
        self.frame_count = 0 

        self.removeduplicates = []
        
        #checkpoint code
        self.activitycheckpoint = 500
        self.checkpoint_history = {} # mapping of Object id -> Region -> [ [frameids], [frametimes], [type of object or list types of keypoints]
        self.checkpoint_idle    = {}
        self.last_flush_frame = 0
        self.hysterisis_length = cloudconfig.posepresence_hysteresis

       
        self.thresholds = {
            "Liquid" : 0.8,
            "Label"  : 0.8,
            "Monitor" : 0.5,
            "L1S1"    : 1.0,
            "L1S2"    : 1.0,
            "R7"      : 0.6
        }
        # self.checkpoint_interval = 30
        # f_stop = threading.Event()
        # self.f(f_stop)

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


    def send_to_db(self,trackid,match,starting,ending,startingsource,endingsource,relativeframetime,relativending):

        """
        Sending data to db 
        """        
        # debug print
        #print(match)
        if(cloudconfig.multiple_json_flag==True):
            totaltime=relativending-relativeframetime
        else:
            totaltime = ending - starting
        #small filter to prevent bad ones to not be sent
        if totaltime <= self.data[match][-2]: #checking tmin
            return
    
        # print('SENDING DATA TO DB !!!!!')
        ret = {
                'trackid' :   trackid, 
                'match'   :   match, #this is the current timestamp of when message was seen
                'startime'  :   '2021-03-23 ' +  str(datetime.timedelta(seconds=starting)),
                'endtime' :     '2021-03-23 ' + str(datetime.timedelta(seconds=ending)),
                'totaltime' : totaltime,
                'relativestarting' : '2021-03-23 ' + str(datetime.timedelta(seconds=relativeframetime)),
                'relativending' : '2021-03-23 ' + str(datetime.timedelta(seconds=relativending)),
                'endingsource' :   endingsource,
                'startingsource' : startingsource
                
            }

        #sending data to presence table
        self.send_match(ret)
        # print('value sent')

    def send_frame_state_todb(self,framestate,frameid=None):

        sample = {
            'frameid' : None,
            'trackid'  : None,
            'delta'    : None,
            'deltatime' : None,
            'pa' : None,
            'pax' : None,
            'pay' : None,
            'pb' : None,
            'pbx' : None,
            'pby' : None,
            'pc' : None,
            'pcx' : None,
            'pcy' : None,
            'pd' : None,
            'pdx' : None,
            'pdy' : None,
            'pe' : None,
            'pex' : None,
            'pey' : None,
            'pf' : None,
            'pfx' : None,
            'pfy' : None,
            'pg' : None,
            'pgx' : None,
            'pgy' : None,
            'ph' : None,
            'phx' : None,
            'phy' : None,
            'pi' : None,
            'pix' : None,
            'piy' : None,
            'pj' : None,
            'pjx' : None,
            'pjy' : None,
            'pk' : None,
            'pkx' : None,
            'pky' : None,
            'pl' : None,
            'plx' : None,
            'ply' : None,
            'pm' : None,
            'pmx' : None,
            'pmy' : None,
            'pn' : None,
            'pnx' : None,
            'pny' : None,
            'po' : None,
            'pox' : None,
            'poy' : None,
            'pp' : None,
            'ppx' : None,
            'ppy' : None,
            'pq' : None,
            'pqx' : None,
            'pqy' : None,
            'pr' : None,
            'prx' : None,
            'pry' : None, 
        }

        #frame_count = self.frame_count
        delta = 1/cloudconfig.infer_fps
        frame_time = delta * frameid

        retdict = {}
       

        for id in framestate:
           
            trackid = list(id)[0] # 0e -> 0 #fix trackidk thing

            #check if track id exists in final json
            if trackid not in retdict:
                retdict[trackid] = sample.copy()


            # print(id,framestate[id])
            # print('****************')
            frameid = frameid
            delta = delta
            delta_time = frame_time
            currentstate = framestate[id]['current_state']
            keypointname = self.object_type_mappings[framestate[id]['object_type']]

            kpx = keypointname + 'x'
            kpy = keypointname + 'y'

            retdict[trackid]['frameid'] = frameid
            retdict[trackid]['delta'] = delta
            retdict[trackid]['trackid'] = trackid #framestate[id]['trackid'] # this is the track id 
            retdict[trackid]['deltatime'] = delta_time
            retdict[trackid][keypointname] = currentstate[0] if len(currentstate) > 0 else None

            retdict[trackid][kpx] = framestate[id]['centroid'][0]
            retdict[trackid][kpy] = framestate[id]['centroid'][1]


            #print(framestate[id])
            # print(trackid,frameid,delta,delta_time,keypointname,currentstate)
            # print('**********************')


        
        #print(final,'*$#############')
        for val in retdict:
            # pass
            # #print(val,'&&&&',final[val],'BOOM')
            # #UNCOMMENT
   
            print('frame id after bring processed sent to DB')
            print(retdict[val])
            print('************************************')
            self.send_tempobjectid(retdict[val])


        
    def check_intersection(self,list1,list2):
        """
        list1 - > [1,2,3]
        list2 ->  [2,4,6]
        ret [2]
        """
        return set(list1).intersection(list2)

        
    def reformat_framestate(self,framestate):
        """
        Reformat framestate
        """
        reformatted_dict = {}
        data = framestate
        for keypoint in data:
            kpinfo = data[keypoint]
            trackid =  ''.join(list(kpinfo['object_id'])[:-1]) #7c -> 7
            for region in data[keypoint]['current_state']:
                #first time seeing new track id in reformatted_dict
                keypoint_object_type = self.object_type_mappings[data[keypoint]['object_type']] # object type between 0 -> 17
                if trackid not in reformatted_dict:
                    reformatted_dict[trackid] = {region: [keypoint_object_type]}
                    continue
                #first time seeing region in trackid in reformatted dict
                if region not in reformatted_dict[trackid]:
                    reformatted_dict[trackid][region] = [keypoint_object_type]
                    continue

                #already have seen trackid and object type
                reformatted_dict[trackid][region].append(keypoint_object_type)

        return reformatted_dict

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




    def flush(self,currframeid):
        """
        This function runs once every {x} number of frames
        """
        #iterate through checkpoint history
        #go through each object id
        remove = []
        for object_id in self.checkpoint_history:
            #go through each region for each object id
            if len(self.checkpoint_history[object_id]) == 0:
                remove.append((object_id,None))
            for region in self.checkpoint_history[object_id]:
                #then get the list of frameids appended in this log for this region
                lastframeid = self.checkpoint_history[object_id][region][0][-1] #this gets the last frameid
                #lastframeid -> 1,R7,100
                #curr frameid -> 600
                difference = currframeid - lastframeid
                if difference > self.hysterisis_length:
                    #send to db
                    startingsource, starting, endingsource,ending = self.getstartminmax(self.checkpoint_history[object_id][region][1])
                    relativeframetime = min(self.checkpoint_history[object_id][region][3])
                    relativending = max(self.checkpoint_history[object_id][region][3])
                    #flush messages also gets added to queue
                    self.send_to_db(object_id,region,starting,ending,startingsource,endingsource,relativeframetime,relativending)

                    #now remove only the region from the respective id from checkpoint history
                    #object id - > region - > [[frameid], [frametimes]
                    remove.append((object_id,region))

        
        #go through queue here and add overlapping logic here
        #problems; while iterating through queue, more elements could be added
        """
        0) Create a temp queue (once processed will be sent to the db)
        1) sort all potential table rows by starting 
        2) then for each row compare the ending column with the next row's starting column 
        3) If overlapping or within a certain delta of 1-2 seconds 
            True  -> Set temp  row's ending to the current rows ending 
            False -> Then add the current temp queue 
        4) continue



        """

        #removing outdated values in checkpoint history
        for val in remove:
            objectid,region = val
            #Region is none means remove objectid
            if region is None:
                self.checkpoint_history.pop(objectid,None)
            else:
                self.checkpoint_history[objectid].pop(region,None)

            

    def process_frame_state(self,framestate,frameid=None,source=None,relativeframeid=None):
        """ 
        process the frame state to understand when an id has left a certain region
        First time seeing state, then create a mappings

        if an id does not appear in the same state agin in a future frame, then
        it is over for that id - then send to db
        """

        frame_time = self.delta * frameid
        
        if(cloudconfig.multiple_json_flag==True):
            relative_frame_time = self.delta * relativeframeid
            checkpoint_frame_id = relativeframeid

        else:
            relative_frame_time = frame_time
            checkpoint_frame_id = frameid

        # print(frame_time,'----->',relative_frame_time)

        # debug prints
        # print(frame_time)
        if(frameid%500==0):
            print("frame",frameid)
            
        self.frame_count += 1

        #handle flushing code
        if checkpoint_frame_id > (self.last_flush_frame + 500):
            self.flush(checkpoint_frame_id)
            self.last_flush_frame = checkpoint_frame_id

        """
        Centroid trackid ID algo hysterisis ->500
        Pose Presence -> 500

        Frame 1 > Trackid 1 -> Pose Presence -> 1 : 501
        Frame 3 > Trackid 2 -> Pose Presence -> 2 : 503

        *******

        Centroid trackid ID algo hysterisis ->500
        Pose Presence -> 500
        
        Frame 1 > Trackid 1 -> Pose Presence -> 1 : 501
        Frame 7 > Trackid 1,2 -> Pose Presence -> 2 : 503 (tmin kicks in for trackid 2)


        Trackid 1 

        Frame 1 -> R1; timestamp
        Frame 2 -> R1; timestamp
        Frame 3 -> R1; timestamp
        Frame 4 ....
        Frame 5
        Frame 6
        Frame 7      R1
        Frames 8 - 10 (Hystersis)
        Frame 11  
        Frame 12
        Frames 13 - 16 (Hysterisis)
        Frame 17
        Frame 18
        Frame 19
        Frame 20
        Frame 21


        """

        framestate = self.reformat_framestate(framestate)

        # TODO: no need to append to checkpoint history
        #print(framestate)
        for object_id in framestate:
            for region in framestate[object_id]:
                setofkeypoints_framestate = framestate[object_id][region] #this gets set of points from the current framestate

                #incase we capture region we dont care from the ui 
                if region not in self.data:
                    continue

                setofkeypoints_ui = self.data[region][0] #this gets set of points from the ui
                intersection_set = self.check_intersection(setofkeypoints_framestate,setofkeypoints_ui)

                #check for intersection match between framestate set and ui set
                if len(intersection_set) >= self.data[region][-1]: #compares to subset count with ui

                    #object id - > region - > [[frameid], [frametimes],[keypoints]]
                    # checkpoint_frame_id is relative to the start of the first video
                    """
                    frametime - > 
                    {
                        filesource1 : some format,
                        filesource2  : someformat,
                    }

                    min(min(filesources)), max(max(filesources)) - > send to db

                    Then set dictionary of frametimes to empty again
                    """

                    if object_id not in self.checkpoint_history:
                        #create a new mapping for timestamps, sources; mapping from id -> region -> source
                        self.checkpoint_history[object_id] = {region : [ [checkpoint_frame_id], { source: [frame_time]} ,source, [relative_frame_time] ]}
                        continue

                    if region not in self.checkpoint_history[object_id]: # [['pa','pb','pd'],0.5]
                        #add frameid
                        self.checkpoint_history[object_id][region] = [ [checkpoint_frame_id], { source: [frame_time]}, source,[relative_frame_time]]
                        continue
                
                    #check for source in dictionary
                    if source not in self.checkpoint_history[object_id][region][1]:
                        self.checkpoint_history[object_id][region][1][source] = [frame_time]

                    #need to check if currstate already exists in self.checkpoint_history[objectid]
                    #then append the keypoint type to the current state for that frame

                    #get last frameid
                    lastframeid = self.checkpoint_history[object_id][region][0][-1] #this gets the last frame id

                    #if current frame id is the same as lastframeid there is more to process
                    if checkpoint_frame_id == lastframeid:
                        continue

                    # print('########### this is self.data')
                    # print(self.data) 200 < 85 + 35
                    # if current frameid is less then last frameid + hysterisis -> all good
                    # if current frameid is greather then last frame + hysterisis -> not good
                    hysterisis_bool = checkpoint_frame_id < lastframeid + self.hysterisis_length #in this case the hysterisi is whatever

                    #if hysterisis book is true and number of keypoints match up to total len of self.data keep going
                    if hysterisis_bool: ## [['pa','pb','pd'],0.5]
                        #update last frameid
                        self.checkpoint_history[object_id][region][0].append(checkpoint_frame_id) #[1,3,5,7]
                        #also append frametime too; 1 is the dictionary; source is the key ---->
                        self.checkpoint_history[object_id][region][1][source].append(frame_time)
                        #also append relative frametime too
                        self.checkpoint_history[object_id][region][3].append(relative_frame_time)

                    #send to db 
                    elif not hysterisis_bool:
                        # print(frameid,lastframeid,len(self.data[region][0]),'&*&*&*&*&*&*&*&*&*&')
                        #extract min,max and sources from object_region_time
                        startingsource, starting, endingsource,ending = self.getstartminmax(self.checkpoint_history[object_id][region][1])

                        # starting = min(self.checkpoint_history[object_id][region][1])
                        # ending =  max(self.checkpoint_history[object_id][region][1])
                        relativeframetime = min(self.checkpoint_history[object_id][region][3])
                        relativending= max(self.checkpoint_history[object_id][region][3])
                        #add to some queue
                        self.send_to_db(object_id,region,starting,ending,startingsource,endingsource,relativeframetime,relativending)

                        #reset checkpoint history
                        self.checkpoint_history[object_id][region][1] = {source : [frame_time]} #reset frame time
                        self.checkpoint_history[object_id][region][3] = [relative_frame_time] #reset relative frame time
                        self.checkpoint_history[object_id][region][0] = [checkpoint_frame_id]     #reset frameid
                      
                    
                    else:
                        pass

                
                #intersection did not match criteria
                else:
                    continue


        # debug prints
        #print('***********************************')
        #print('***********************************')
        return 



            
            
          

    