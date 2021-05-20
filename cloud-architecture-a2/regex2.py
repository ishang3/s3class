import datetime
import re
import regex
from shapely.geometry import Point
import cloudconfig

class Regex:

    def __init__(self,rule,rule_maps=None):
        """
        """
        self.rules = rule  #this # a mapping between object type and rules
        # example of rules_maps
        #{'pa': 'S2S1'}
        self.rules_maps = rule_maps
        self.delta = 1 / cloudconfig.infer_fps #need to fix to config json
        self.partials = {}
        self.currentstates = {}
        self.currentpatterns = {}
        self.times = {}
        self.centroidblob = {}
        self.length = {}
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
             17     : 'pr',
             18     : ('pj', 'pk')
        }


    def add_rule(self,rule):
        """
        Adds a new regex to the current list

        Parameters
        __________
        rule: String
            corresponding regex pattern
        """
        self.rules.append(rule)

    def match2(self,framestate,frameid=None,source=None,relativeframeid=None):
        """

        """
        # debug prints
        #print("start of match2..")

        ret_value = False
        frame_time = self.delta * frameid
        if(cloudconfig.multiple_json_flag==True):
            relative_frame_time = self.delta * relativeframeid
        else:
            relative_frame_time = frame_time

        #print(frame_time,'This is the frame time')
        # print('DEBUG DEBUG DEBUG DEBUG')
        # print(self.currentpatterns)
       
        #1k or 2j 
        for id in framestate:

            #first time seeing object id
            if id not in self.currentpatterns:
                #getting current states by this particular id
                curr_states = framestate[id]['current_state']
                self.currentpatterns[id] = {
                    'current_states' : [[x] for x in curr_states], #[[R0],[R1]]
                    'object_type'    : framestate[id]['object_type'],
                    'frame_time'     : [frame_time], #[framestate[id]['frame_time']]
                    'relative_frame_time' : [relative_frame_time]
                }

            #second or later time seeing object id, update current states with new states
            else:
                #also append time as well
                self.currentpatterns[id]['frame_time'].append(frame_time)#framestate[id]['frame_time'])
                self.currentpatterns[id]['relative_frame_time'].append(relative_frame_time) 
                #print(self.currentpatterns[id]['current_states'],'*#$**#$#$*$#*')
                id_pattern = self.currentpatterns[id]['current_states']
                #there can be multiple patterns for a given id such as [['R1'],['R2'],['R3']]
                temp = []
                for pattern in id_pattern:
                    for state in framestate[id]['current_state']:
                        temp.append([pattern[-1],state])

                #since no new states, nothing should be appended because
                if len(temp) == 0:
                    continue
                self.currentpatterns[id]['current_states'] = []
                for x in temp:
                    self.currentpatterns[id]['current_states'].append([''.join(x)])

            #print(self.currentpatterns[id]['current_states'],'**********',id)

        for id in self.currentpatterns:
            object_type = self.currentpatterns[id]['object_type']
            #if class object is not in mappings, then continue on to the next message
            if object_type not in self.object_type_mappings:
                continue
            obj_nickname = self.object_type_mappings[object_type] #pack -> ('pa,'pb','pc')
            for rule in self.rules_maps:
                #print('$#**#$*#**$#',rule,obj_nickname,'*$*#*$#**$$*#*#$*$#*#$*#$*#$',id)
                #checking if the rule type matches the id type
                if obj_nickname == rule:
                    # print(self.currentpatterns[id],'!!!!!!',id)
                    # print('*************')
                    #getting all the rules associated with that object
                    ruleslist = self.rules_maps[obj_nickname]
                    for checkrule in ruleslist:
                        #this is going through all the states of the id
                        for pat in self.currentpatterns[id]['current_states']:
                            pattern = regex.compile(checkrule)
                            ret = pattern.fullmatch(pat[0],partial=True)

                            #there is no match
                            if ret is None:
                                continue

                            #if partial match is true
                            if ret.partial == True:
                                # do something here if partially done
                                if id not in self.partials: 
                                    self.partials[id] = [pat[0]]
                                else:
                                    self.partials[id].append(pat[0])
                                    self.partials[id] = list(set(self.partials[id]))

                                #print(self.partials)
                                #print('partial !!!!!!!!!!!!!!')

                            #if a match is successful
                            if ret.partial == False:

                                if len(self.currentpatterns[id]['frame_time']) > 0:
                                    startime = min(self.currentpatterns[id]['frame_time'])
                                    endtime = max(self.currentpatterns[id]['frame_time'])
                                    #getting for multiple json
                                    relativeframetime = min(self.currentpatterns[id]['relative_frame_time'])
                                    relativeframetime_end = max(self.currentpatterns[id]['relative_frame_time'])
                                    if(cloudconfig.multiple_json_flag==True):
                                        totaltime = relativeframetime_end-relativeframetime
                                    else:
                                        totaltime = endtime - startime
                                    print(id,totaltime,checkrule)
                                    ret_value = True
                                    payload = {
                                        'startime' : '2021-03-23 ' + str(datetime.timedelta(seconds=startime)),
                                        'endtime' :  '2021-03-23 ' + str(datetime.timedelta(seconds=endtime)),
                                        'totaltime' : totaltime,#totaltime.total_seconds(),
                                        'trackid'  : id,
                                        'match'     : checkrule,
                                        'endingsource'    : source,
                                        'startingsource'   : None, # TODO:Need to be fixed
                                        'relativestarting' :  '2021-03-23 ' + str(datetime.timedelta(seconds=relativeframetime)),
                                        'relativending'    :  '2021-03-23 ' + str(datetime.timedelta(seconds=relativeframetime_end))
                                    }
                                    self.currentpatterns[id]['frame_time'] = []
                                    self.currentpatterns[id]['relative_frame_time'] = [] #empties


        #resetting the current patterns based off the current framestate
        for id in framestate:

            #need to fix this to not clear states where the current frame state is empty
            current_state_frame = framestate[id]['current_state']

            #state is no where zone, so should be remained to the last region been at
            if current_state_frame == []:
                continue

            self.currentpatterns[id]['current_states'] = []

            for curr in current_state_frame:
                self.currentpatterns[id]['current_states'].append([curr])
            for part in self.partials:
                if part == id:
                    self.currentpatterns[id]['current_states'].extend([self.partials[part]])

        self.partials = {}

        #if true then this will return the match value to send too
        if ret_value:
            # debug prints 
            #print("payload", payload)
            #print("########")
            return payload
        #this will return false if no match
        return ret_value



    def match(self,state,id,time,centroid=None):
        """
        Each currentstates[ID] will be associated with a pattern list
        This will now be an entire frame level analysis no longer a single object match
        """

        id = str(id)

        #need to clean up code
        if id not in self.currentstates:   #this should be called current pattern across times
            self.currentstates[id] = state #this is list of patterns with respect to time, and each pattern is a list
                                           #hence it is a lists of lists
            """
            We call this patternlists
            lets say the object o is in R1, R2, and R3, so the lists of lists is below
            [[R1],[R2],[R3]]
            There is only 1 element in sublists, becuase it is the first time
            """
        else:
            """
            suppose the current states for this id are R2, R3 then the current states will become 
            R1R2, R1R3, R2R2, R2R3, R3R2, R3R3
            cal a function called update_id_patterns
            In this function, get pattern listfor this 
            example =  self.currentstates[id]
            newlist = []
            for each pattern in example:
                for each  state in framestate: ()
                    newpattern = pattern.append(state)
                    newlist.append(newpattern)
            
            self.currentstates[id] = newlist
                    
                    
            """
            self.currentstates[id] += state  #this will be a list of a lists because a id can belong in multiple states
                                             #{1 : [[R1,R1],[R3,R1]]

        #this is appending the times based off the ids
        if id not in self.times:
            self.times[id] = [time]
        else:
            self.times[id].append(time)

        #adding the centroid to the respective id blob
        # if id not in self.centroidblob:
        #     self.centroidblob[id] = [centroid]
        # else:
        #     self.centroidblob[id].append(centroid)
        # #this is getting the running total of the distance from the centroids
        # if id not in self.length or len(self.centroidblob[id]) == 1:
        #     self.length[id] = 0
        # else:
        #     self.length[id] += Point(self.centroidblob[id][-1]).distance(Point(self.centroidblob[id][-2]))

        #check if there is a match
        """
        uselessidlist={}
        for rule in self.rules:
            #usefuleid=0???
            for id in self.currentpatterns:
                #set nevereneteredbefore=1 . this variable is explained below. Only once per id.
                if id.type == rule.type:
                    usefulid=1 ??
                    for each pattern in id: #this will be a list of lists as mentioned above
                        if re.match(rule,pattern): 
                            #send data to db
                            Then also replace that pattern with current objectstate, then set neverenteredbefore = 0
                        else: #this calls when there is no match but only once!! so create a variable called neverenteredbefore
                            #if(neverenteredbefore) do below
                            #remove this pattern from the list, and then add a pattern with single state which is current frame state
                            #basically extract the current state from framestate's corresponding id
                            #this is only done once for each id so set neverenteredbefore=0
                  if(!usefulid) uselessid.append {id}  ??? 
                    
        """
        for rule in self.rules:
            if re.match(rule,self.currentstates[id]):
                startime = min(self.times[id])
                endtime = max(self.times[id])
                totaltime = endtime - startime
                ret = {
                    'match' :self.currentstates[id],
                    'totaltime' : totaltime,
                    'trackid' : id,
                    'sourceid' : 0,
                    'starting' : min(self.times[id]),
                    'ending' : max(self.times[id]),
                    'length' : 0,#self.length[id],
                    'speed' : 0,#self.length[id] / totaltime.total_seconds()

                }
                print('MATCH',id,self.currentstates[id],'Total time:',totaltime,'Starting time :',startime,'End Time:',endtime)
                self.times[id] = [time]
                self.currentstates[id] = state
                self.length[id] = 0
                self.centroidblob[id] = []

                return ret

        #there is no rule matched so reset to the current state
        self.currentstates[id] = state #this code will need to be in the if statement checking if an object type matches the rule type
        return None



"""
Notes:
Partial Regex Matching
https://stackoverflow.com/questions/36145045/partial-regex-matching
"""

