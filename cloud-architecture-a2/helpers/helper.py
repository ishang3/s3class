from operator import itemgetter

body_labels = {0:'a',
             1: 'b', 
             2: 'c', 
             3: 'd', 
             4: 'e', 
             5: 'f',
             6: 'g',
             7: 'h',
             8: 'i', 
             9: 'j',
             10: 'k',
             11: 'l',
             12: 'm',
             13: 'n',
             14: 'o',
             15: 'p', 
             16: 'q',
             17: 'r'}


inverse_body_labels = {
        'a' : '0',
        'b' : '1',
        'c' : '2',
        'd' :  '3',
        'e' :  '4',
        'f' :  '5',
        'g' :  '6',
        'h' :  '7',
        'i' :  '8',
        'j' : '9',
        'k' : '10',
        'l' :  '11',
        'm' :  '12',
        'n' :  '13',
        'o' :  '14',
        'p' :  '15',
        'q' :  '16',
        'r' :  '17'
}

object_type_mappings = {
              'pa' :  '0',
              'pb' :  ' 1',
             'pc'  :  '2',
              'pd' : '3',
              'pe' : '4',
              'pf' :  '5',
              'pg' :  '6',
               'ph' :  '7',
               'pi' :  '8',
               'pj' :  '9',
               'pk' :  '10',
               'pl' :   '11',
               'pm' :   '12',
               'pn' :   '13',
               'po' :  '14',
               'pp' :  '15',
               'pq' :  '16',
               'pr' :  '17'
        }


bodypart_to_letter = {
              'Nose' :  'pa',
              'Left Eye' :  'pb',
             'Right Eye'  :  'pc',
              'Left Ear' : 'pd',
              'Right Ear' : 'pe',
              'Left_shoulder' :  'pf',
              'Right_shoulder' :  'pg',
               'Left_elbow' :  'ph',
               'Right_elbow' :  'pi',
               'Left_wrist' :  'pj',
               'Right_wrist' :  'pk',
               'Left_hip' :   'pl',
               'Right_hip' :   'pm',
               'Left_knee' :   'pn',
               'Right_knee' :  'po',
               'Left_ankle' :  'pp',
               'Right_ankle' :  'pq',
               'neck' :  'pr'
}


def get_bbox(kp_list):
    bbox = []
    for aggs in [min, max]:
        for idx in range(2):
            bound = aggs(kp_list, key=itemgetter(idx))[idx]
            bbox.append(bound)
    return bbox

# TODO: fix this
def filter_keypoints(kp,payload):
    """
    This will return a list of keypoints based off the user wants only

    """
    active_keypoints = list(payload['pose_regex_type'].keys()) # -> [ pa,pb, (pa,pb)] #flatten this list()
    check = []
    ret = {}
    for active in active_keypoints:
   
        check.append(object_type_mappings[active])
    
    for val in check:
        
        if val not in kp:
            continue
        ret[val] = kp[val] 
    
    return ret
   

#this will take in a list of keypoints and return a bounding box
def convert_kp(kp):
    if len(kp[0]) == 0:
        return
    new = []
    for tup in kp[0]:
        val = kp[0][tup]
        new.append(val)
    return get_bbox(new)


'''
Given kps for one person, return the centroid (Cx,Cy) of all keypoints except wrists and elbows

wrists: 7,8
elbows: 9,10

what kps looks like:
{'0': [321.2842102050781, 182.49850463867188], '1': [329.1330261230469, 175.37741088867188], 
'2': [318.6502990722656, 174.97030639648438]}
'''
def get_core_centroid(kps)->tuple:
    rejectIDs = [7,8,9,10] # exclude wrists and elbows from calculation
    sumX = 0
    sumY = 0
    numRejects = 0 # number of keypoints in kps that were excluded
    for kpID in kps:
        if(int(kpID) in rejectIDs):
            numRejects += 1
            continue
        coord = kps[kpID] # [x,y]
        sumX += coord[0]
        sumY += coord[1]
    
    Cx = int(sumX / (len(kps)-numRejects)) # x coord of centroid
    Cy = int(sumY / (len(kps)-numRejects)) # y coord of centroid
    return (Cx,Cy)





