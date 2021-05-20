import numpy as np
from PIL import Image
from math import sqrt
import json
import ast
from sqlops.sendmessage import MessageSending
import datetime

class Idle(MessageSending):
    def __init__(self,data,table_name,presence_table):
        """
        Clean this up where I remove the unused stuff
        https://stackoverflow.com/questions/51509808/how-to-measure-distances-in-a-image-with-python
        """
        MessageSending.__init__(self,table_name,presence_table)
        #data input with reges
        self.data = data

        self.width = 1920
        #this will tell me what objects to capture the idle state for
        #self.regex = idle_regex
        #create a mapping between id's and their last observed keypoint
        self.mappings_last_keypoint = {}
        #mappings between keypoints and their last centroid, timestamp
        self.mappings_last_distance = {}
        #create a mapping between ids and their object type
        self.mappings = {}
        #a frame interval to determine when to calculate the pixel distance

    
    def calculate_pixel_distance(self,p1,p2):
        x1,y1 = p1
        x2,y2 = p2
        pixel_distance = sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)
        #print(pixel_distance,'*****',pixel_distance/self.width)
        return pixel_distance

    
    def insert_distance(self,id,distance,timestamp):
        """
        keeping a list of distances per id per interval of frames to understand when sudden movement has occured
        {0: [21.213203435596427, 774.0471561862365],
        """
        if id not in self.mappings_last_distance:
            self.mappings_last_distance[id] = [(distance,timestamp)]
        else:
            self.mappings_last_distance[id].append((distance,timestamp))

    
    def process_current_distances(self,):
        """
        After all the data for each frame ahs been processed, 
        this will go through the mappings_last_distance and check if the last 2 distances
        have been large indicating person has moved since last seen
        """
        
        for id in self.mappings_last_distance:
            val = self.mappings_last_distance[id]

            if len(val) >= 2:
                
                #this is the first value of 
                first = val[0]

                #this is getting the latest data from the 
                last = val[-1]
                distance_last,timestamp_last = last

                if distance_last > 100:
                    #print(id,val,'************')
                    payload = {
                        'startime' : datetime.datetime.now(),#timestamp,
                        'endtime' :  datetime.datetime.now(),
                        'totaltime' : 3, #- timestamp,
                        'trackid'  : id,
                        'match'    : 'p-idle' #because mapping between R0 - > R0 region Presence
                    }
                    self.send_match(payload)
                    #send to db
                    self.mappings_last_distance[id] = []

    def process_total(self,total,mappings,timestamp):
        
        #this always updates the mappings between the ids and their object type 
        self.mappings = mappings
        for val in total:
            
            id = val[0]
            centroid = val[1]

            if id not in self.mappings_last_keypoint:
                self.mappings_last_keypoint[id] = [1,centroid]
                self.insert_distance(id,0,timestamp)
            else:
                if self.mappings_last_keypoint[id][0] % 200 == 0:
                    #print(self.mappings_last_keypoint[id],centroid)
                    old_centroid = tuple(self.mappings_last_keypoint[id][1])
                    current_centroid = tuple(centroid)

                    #calculate distance between the last centroid and the new one - 150 frames apart
                    distance = self.calculate_pixel_distance(old_centroid,current_centroid)

                    #insert distance 
                    self.insert_distance(id,distance,timestamp)
 
                    #centroid gets updated
                    self.mappings_last_keypoint[id][1] = centroid
                #id gets updated regardless
                self.mappings_last_keypoint[id][0] += 1
        

        #now after all the data has been processed, need to go through and see which pair of points have a large distance 
        self.process_current_distances()



    def process_object_data(self,data):
        pass


# import json

# idle = Idle()
# with open('object_data_boom.json') as json_file:
#     data = json.loads(json_file.read())
#     res = ast.literal_eval(data) 
#     for val in res:
#         total = val[0]
#         mappings = val[1]
#         timestamp = val[2]
       
#         idle.process_total(total,mappings,timestamp)
        #print('**********')
        # print(mappings)
        #exit()
    
    
        
        

    
