from sqlops.sendmessage import MessageSending
import datetime


class RegionPresence(MessageSending):
    def __init__(self,data,table_name,presence_table):
        MessageSending.__init__(self,table_name,presence_table)
        self.data = data
        self.prev_region_mappings = {}
        self.current_frame_objectids = []


    def clean_up_previous_frames(self,):
        """
        this removes any lingering object ids in any states if they arent present in the current frame

        """
        for region in self.prev_region_mappings:
            remove = []
            for objid in self.prev_region_mappings[region]:
                if objid not in self.current_frame_objectids:
                    remove.append(objid)
            for val in remove:
                print('REMOVING THIS VALUE FROM',region,val)
                self.prev_region_mappings[region].pop(val,None)

    
    def remove_prev_empty_states(self,empty):

        for region in self.prev_region_mappings:
            for val in empty:
                self.prev_region_mappings[region].pop(val,None)


    def update_previous_frame_state(self,framestate,donotadd,emptystate):

        """
        This updates the mappings of the framestate wrt to the previous frame

        NEED to prevent from updating ids that were removed

        Frame 1 -> (Id: 1, R0, True)
        Frame 2 -> (Id: 1, R0, True) -> (Id: 1, R0, False)
        Frame 3 -> (Id: 1, R0, False) -> ID 1 gets removed -> 
        Frame 4 -> (Id: 1, R0, False)
        """

    

        for id in framestate:

            current_state = framestate[id]['current_state']
            object_id = framestate[id]['object_id']
            object_type = framestate[id]['object_type'].lower() 
            timestamp = framestate[id]['frame_time']

            #this prevent the previous ids from being used as false
            if object_id in donotadd:
                continue


            #if currentstate is empty 
            if len(current_state) == 0:
                continue

            #first check if object type exists
            if object_type not in self.data:
                continue
        
            #then check if state exists for that object type
            if current_state[0] not in self.data[object_type]:
                continue


            #if current state does not exist then addd everything 
            if current_state[0] not in self.prev_region_mappings:
                self.prev_region_mappings[current_state[0]] = {
                    object_id : [False,(timestamp,object_type)] #list of [Boolean, ObjectProperties]

                }
                continue

            #if current state but id does not exist then add both boolean and metadata
            if object_id not in self.prev_region_mappings[current_state[0]]:
                self.prev_region_mappings[current_state[0]][object_id] = [False,(timestamp,object_type)]
                continue
            
            #if id and current state exist then only change the Boolean not the timestamp
            
            self.prev_region_mappings[current_state[0]][object_id][0] = False
        
        #clean up previous frame object to remove any ids that have an empty state 
        self.remove_prev_empty_states(emptystate)

    
    def process_frame_state(self,framestate):
        """
        process the frame state to understand when an id has left a certain region

        First time seeing state, then create a mappings

        if an id does not appear in the same state agin in a future frame, then
        it is over for that id - then send to db
        """
        #print(framestate,'*****')
        #reset for every frame state
        self.current_frame_objectids = []
        empty_state_ids = []
        for id in framestate:
            current_state = framestate[id]['current_state']
            object_id = framestate[id]['object_id']
            object_type = framestate[id]['object_type']
            timestamp = framestate[id]['frame_time']

            #this keeps an up to date list of the object ids present in this frame
            self.current_frame_objectids.append(object_id)

            if len(current_state) == 0:
                empty_state_ids.append(object_id)
                continue

            #NEEDS TO BE FIXED to handle multiple states
            #here check previous frame data to match current frame
            if current_state[0] in self.prev_region_mappings:
                #check if current id exists previous state
                if object_id in self.prev_region_mappings[current_state[0]]:
                    #if exists then set id to true 
                    self.prev_region_mappings[current_state[0]][object_id][0] = True  #[Boolean, ObjectProperties]

        #clean up any old lingering ids in previous frame
        #self.clean_up_previous_frames()      
        
        #send all False ids to db then delete from prev_region_mappings
        donotadd = self.send_dead_ids()

        #this updates the mappings of the previous state To false at the end of each frame
        self.update_previous_frame_state(framestate,donotadd,empty_state_ids)

        #clean up any old lingering ids in previous frame
        #self.clean_up_previous_frames()  

        
       
    
    def send_dead_ids(self,):
        remove = []
        ret_remove = []
        #iterate through all the states
        for state in self.prev_region_mappings:
            for object_id in self.prev_region_mappings[state]:
                #print(self.prev_region_mappings)
                #print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
                #print(state,'@!@!!@!',object_id)
                
                if self.prev_region_mappings[state][object_id][0] == False:
                    # print('**********')
                    # print('**********')
                    # print('**********')
                    print('**********','data being sent to the database',object_id)
                   #print('!!!!!!!!!!!!!',self.prev_region_mappings)
                    timestamp,object_type = self.prev_region_mappings[state][object_id][1]

                    #end time is now:
                    end_time = datetime.datetime.now()  

                    #parsing muvvas timestamp
                    # val = int(str(timestamp)[:10])
                    # starting =  datetime.datetime.fromtimestamp(val)
                    payload = {
                        'starting': datetime.datetime.now(), #starting,#timestamp,
                        'ending':   end_time,
                        'type'  :   'regionpresence',
                        'trackid'  : object_id,
                        'day' :      int(end_time.today().weekday()),
                        'timeshort' : end_time.strftime("%H:%M:%S"),
                        'objectvalue'    : state, #because mapping between R0 - > R0 region Presence,
                    }
                    self.send_objectid(payload)
                    #removing id from region mappings
                    remove.append((object_id,state))
                    ret_remove.append(object_id)
        
        for val in remove:
            remove_objid,remove_state = val
            self.prev_region_mappings[remove_state].pop(remove_objid,None)

        return ret_remove
                   
                
