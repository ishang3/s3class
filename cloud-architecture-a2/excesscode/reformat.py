import json 


data = json.load(open('framestate.json', 'r'))

reformatted_dict = {}

for keypoint in data:
    print(keypoint,data[keypoint])
    kpinfo = data[keypoint]
    trackid =  ''.join(list(kpinfo['object_id'])[:-1]) #7c -> 7
    for region in data[keypoint]['current_state']:
        #first time seeing new track id in reformatted_dict
        keypoint_object_type = data[keypoint]['object_type'] # object type between 0 -> 17
        if trackid not in reformatted_dict:
            reformatted_dict[trackid] = {region: [keypoint_object_type]}
            continue
        #first time seeing region in trackid in reformatted dict
        if region not in reformatted_dict[trackid]:
            reformatted_dict[trackid][region] = [keypoint_object_type]
            continue

        #already have seen trackid and object type
        reformatted_dict[trackid][region].append(keypoint_object_type)

    

print(reformatted_dict)


#  { 7r {'object_type': 17, 'object_id': '7r', 'centroid': [1099.0751266479492, 358.2889437675476], 'current_state': ['R1'], 'frame_time': 'None'},
#         7i {'object_type': 8, 'object_id': '7i', 'centroid': [1046.3001251220703, 399.5656943321228], 'current_state': [], 'frame_time': 'None'}
#         }
        
# Post reformatting_ > Trackid -> State -> Keypoints

{'1': {'R7': [11, 12, 13, 14, 15]},
 '2': {'R1': [3, 5, 17]},
'7': {'R1': [0, 1, 2, 3, 4, 5, 6, 7, 9, 17]}}
        


    
