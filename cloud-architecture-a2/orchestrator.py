"""
This class will serve as the driver for setting up a single camera
"""
from geom import Geometry
from regex2 import Regex
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from shapely.geometry import box
import shapely.ops as so
from utils import test
import time
import datetime
import matplotlib.pyplot as plt
import psycopg2
from psycopg2.extensions import adapt, register_adapter, AsIs
from payload.payload import Payload
from sqlops.Postgres import Database
from sqlops.sendmessage import MessageSending
#from utilspytorch import utilspytorch
from regionopresence import RegionPresence
from personpresence.personfinder import PersonFinder
from posepresence.posepresence import PoseFinder
from ergonomics.ergonomicsorch import Ergonomics
import cloudconfig

class Orchestrator(Geometry,Regex,Database,MessageSending):

    def __init__(self,
                region,
                lines,
                rule,
                rule_map,
                commands,
                table_name,
                presence_table,
                region_presence_state,
                region_presence_regex,
                pose_presence_state,
                pose_presence_regex,
                ergonomic_state,
                ergonomic_regex,
                pose=False): # if pose=false, we want pose


        Geometry.__init__(self,region,lines)
        MessageSending.__init__(self,table_name,presence_table)
        Regex.__init__(self,rule,rule_map)
        Database.__init__(self,table_name)
        self.commands = commands

        #Region: Hardcoded Track id
        self.apriori_regions = cloudconfig.region_to_trackid_hardcode

        # self.apriori_regions = {
        #     'Sort' : 1,
        #     'Pack' : 1,
        #     'Base' : 1,
        #     'Cover' : 1
        # } 
        self.kp_rule_map = rule_map
        self.pose_flag = pose
        #this determines if the presence state if it sgood to do the presence region
        self.region_presence_flag = region_presence_state
        self.region_presence = PersonFinder(region_presence_regex,table_name,presence_table) if self.region_presence_flag else False
        #this will create the flags for the pose presence
        self.pose_resgion_flag = pose_presence_state
        self.pose_presence = PoseFinder(pose_presence_regex,table_name,presence_table) if self.pose_resgion_flag else False
        # this will create the flags for ergonomics
        self.ergonomic_flag = ergonomic_state
        self.ergonomic = Ergonomics(ergonomic_regex,table_name,
                                           presence_table) if self.ergonomic_flag else False

    def check_intersection(self,list1,list2):
        """
        list1 - > [1,2,3]
        list2 ->  [2,4,6]
        ret [2]
        """
        return set(list1).intersection(list2)
    
    def _reformat_framestate_activity(self,framestate,frameid):
        """
        Only reformat if pose state is True or false you know...
        need to do some check with incoming payload and see what fits
        take intersection code from pose presence for comparing current with payload 

        """

        timestamp = frameid * (1/cloudconfig.infer_fps)

        retstate = {}
        #HARDCODED CHANG LATER
        delim = 'cycle' #1 -> 1cycle
<<<<<<< HEAD
        regionsofinterest = ['Botpallet','Toppallet']
=======
        regionsofinterest = ['R1','R2']
>>>>>>> 396ef46a1bd497b7f440b7f4121d2ebf27835429

        temp = {}
        #mapping of temp trackid -> region -> keypoints
        for objectid in framestate:
            currentregions = framestate[objectid]['current_state']
            trackid = ''.join(list(objectid)[:-1]) #1a -> 1; 11a -> 11
            keypoint = 'p' + list(objectid)[-1] #a -> pa; b -> pb
            for region in currentregions:
                if trackid not in temp:
                    temp[trackid] = {region : [keypoint]}
                    continue
                if region not  in temp[trackid]:
                    temp[trackid][region] = [keypoint]
                    continue
                
                temp[trackid][region].append(keypoint)

                
        #now iterate through temp and see if there is any overlap with the mul_keypoint spec
        for trackid in temp: #trackid -> 1
            listofkps = temp[trackid]
            # print(listofkps,'*$*$*#*$#*$*#*$#*$#*$#')
            # print(self.kp_rule_map)

            for tup in self.kp_rule_map: # ('pa','pb','pc') : [R0R1]
                print(tup,type(tup),type(tuple('a')),'*****',self.kp_rule_map,'---->',timestamp)
                if type(tup) == type(tuple('a')):
                    #iterate through list of kps {'R1': ['pd', 'pe', 'pf', 'ph', 'pj', 'pl', 'pn', 'pr'], 'R2': ['pg', 'pi', 'pm']}
                    confirmedregions = []
                    for region in listofkps:
                        if region in regionsofinterest:
                            kps = listofkps[region] 
                            ret_set = self.check_intersection(list(tup),kps) #send both the input to compare with the region
                            print(list(tup),kps,'*-------->',ret_set)
                            if len(ret_set) >= 1:
                                confirmedregions.append(region)
                    keytrackid = str(trackid) + delim
                    if len(confirmedregions) > 0: #if 1 or more regions
                        print(keytrackid,confirmedregions,'&$&$&#&$#&$#&#$&$#&$#')
                        retstate[keytrackid] = { #Number + kp 
                            'object_type' :  19,
                            'object_id'   : keytrackid,
                            'centroid'    : (0,0),
                            'current_state' : confirmedregions, #this is a list
                            'frame_time' :   timestamp #datetime.datetime.now() #date_time_obj = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')
                        }
                                
        #add new entries to framestate that match payload requirements
        #1cycle -> 

        #print(temp)

        return retstate

    def _fix_apriori_trackids(self,framestate):
        """
        Filter framestate to do the following 
        1) Change track id to match dictionary above for regions mentioned
        2) If more then 1 track id exists for a given region, randomly remove all of them 
        except 1 (TODO: maybe change this?)
        3) This is then sent to the activity code 

        object id -> 1a,1b,2c -> 1a,1b,1c
        """
        ret = framestate.copy() #copy
        
        #id -> 1a, 1b, 11a,90h
        for id in framestate:
            mul_region_check = []
            #iterate through the regions that obj id is in 
            for region in framestate[id]['current_state']:
                #check if exists in hardcoded and did we see it before
                if region in self.apriori_regions and region not in mul_region_check:
                    #change current track id to hardcoded 1
                    kpname = list(id)[-1] 
                    newid  = str(self.apriori_regions[region]) + kpname
                    mul_region_check.append(region) #we have seen this region

                    ret[newid] = { #Number + kp 
                        'object_type' : framestate[id]['object_type'],
                        'object_id'   : newid,
                        'centroid'    : framestate[id]['centroid'],
                        'current_state' : framestate[id]['current_state'],
                        'frame_time' :    framestate[id]['frame_time']
                    }  

                    ret.pop(id, None)     

        return ret

                


    def _do(self,values,frameid=None,source=None,relativeframeid=None):
        """
        Iterate through the csv here; and put centroid of bounding box
        and then check if the point matches any regions

        then take that region and see if its a match;then keep
        """

        # if self.rules == []:
        #     return

        # debug print
        # print("start of orchestrator _do...")
        #initialize framestate here
        framestate = {}
        ret, mappings, timestamp = values
        for tup in ret:
            objectid,centroid = tup # stage 1 Need to return both type and frame time; either muvva time or relative time from 0
            currentState = self.obj_state(self.create_point(centroid))
            #framestanumte.append{ objectid :(objecttype, objectid, centroid, frametime, currentstates)}
            framestate[str(objectid)] = { #Number + kp 
                        'object_type' : mappings[objectid],
                        'object_id'   : objectid,
                        'centroid'    : centroid,
                        'current_state' : currentState, #this is a list
                        'frame_time' :   timestamp #datetime.datetime.now() #date_time_obj = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')
            }

            #group mapping
            """
            Arrange keypoints in a mapping 
            have some logic here for combining 
            1p,1q - > R1 IFF both keypoints have the same region and track id
            1pq ->
            """
        # this is only reformatting the framestate for the pose ones
        # if self.pose_flag == False:
        #     regexframestate = self._reformat_framestate_activity(framestate)


        #call match code after entire frame with frame state instead of current frame

        #filter framestate BEFORE it gets sent to activity 
        # print(framestate)
        # print('SPACER SPACER SPACER')
        #self._reformat_framestate_activity(framestate,timestamp)
        # print('***$*$*#*$#*$#*#$*$#*$#*#$*$#*$#**$#*$#*$*$#*$')
        
        #this is for changing for 1 track id
        if cloudconfig.hardcode_trackid_flag == True:
            newframestate = self._fix_apriori_trackids(framestate)
        else:
            newframestate = framestate
        
        # reformatted = self._reformat_framestate_activity(framestate,frameid) # for multi kp
        ret = self.match2(newframestate,frameid,source,relativeframeid)

        if ret:
            print('sending activity data')
            #need to send to db
            self.send_match(ret)
            # print(ret['relativestarting'],ret['relativending'])

        #sending data to region presence if is activated
        if self.region_presence_flag:
            self.region_presence.process_frame_state(newframestate,frameid)

        #sending data to pose region if activated
        if self.pose_resgion_flag and self.pose_flag == False:
            self.pose_presence.process_frame_state(newframestate,frameid,source,relativeframeid)

        # sending data ergonomics processing if activated
        if self.ergonomic_flag and self.pose_flag == False:
            self.ergonomic.process_frame_state(newframestate,frameid)







