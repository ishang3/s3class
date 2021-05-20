import argparse
import cv2
import json
from operator import itemgetter
import numpy as np

def get_bbox(kp_list):
    bbox = []
    for aggs in [min, max]:
        for idx in range(2):
            bound = aggs(kp_list, key=itemgetter(idx))[idx]
            bbox.append(bound)
    return bbox

#this will take in a list of keypoints and return a bounding box
#returns (min x,min y,max x, max y)
#uses x,y in standard image coordinates
def convert_kp(kp):
    if len(kp[0]) == 0:
        return
    new = []
    for tup in kp[0]:
        val = kp[0][tup]
        new.append(val)
    return get_bbox(new)

'''
MAIN ENTRY POINT
Given an annotation json, draw the keypoints.
Color according to person
usage:
    change input arguments, then run script
'''

# parse args
ap = argparse.ArgumentParser()
ap.add_argument('-o','--output',default='/Users/ishan/Documents/cloud/cloud-architecture-a2/videout/output.avi',help='path to output video')
ap.add_argument('-wd','--width',default = 640,help='width of video')
ap.add_argument('-ht','--height',default = 480,help='height of video')
ap.add_argument('-f','--fps',default = 29.97,help='fps of video')
args = vars(ap.parse_args())



# init video
fourcc = cv2.VideoWriter_fourcc('M','J','P','G')
vidpath = args['output']
vidwriter = cv2.VideoWriter(
        vidpath,
        fourcc,
        args['fps'],
        (args['width'],args['height']))

# color dict
person_to_color = {
    '0': (255,255,255), # white
    '1': (255,0,0), # red
    '2': (0,255,0), # lime
    '3': (0,0,255), # blue
    '4': (255,255,0), # yellow
    '5': (0,255,255), # cyan
    '6': (255,0,255), # magenta
    '7': (128,0,0), # maroon
    '8': (128,128,0), # olive
    '9': (0,128,0), # green
    '10': (128,0,128), # purple
    '11': (0,128,128), # teal
    '12': (0,0,128), # navy
    '13': (0,0,128) # navy for extra people
}

prevframeid = 0
img = np.zeros((args['height'],args['width'],3),np.uint8)

print("Drawing Keypoints...")

def write_frame(bbox_mappings,mappings,frameid,flag):

    # new canvas
    img = np.zeros((args['height'],args['width'],3),np.uint8)

    # draw frame id
    org = (10,30)
    font = cv2.FONT_HERSHEY_SIMPLEX
    fontScale = 1
    thickness = 2  
    img = cv2.putText(img, str(frameid), org, font, 
    fontScale, (255,255,255), thickness, cv2.LINE_AA)

    count = 0 
    for trackid in bbox_mappings:
        bbox =  bbox_mappings[trackid]

        # set color for this person
        if(int(count) > 12):
            color = person_to_color['13']
        else: 
            color = person_to_color[str(count)]


        # draw bbox
        upperleft = (int(bbox[0]),int(bbox[1]))
        bottomright = (int(bbox[2]),int(bbox[3]))
        rec_color = (128,128,128)
        rec_thick = 2
        img = cv2.rectangle(img,upperleft,bottomright,rec_color,rec_thick)
        img = cv2.putText(img, str(trackid), upperleft, cv2.FONT_HERSHEY_SIMPLEX, 
            0.8, color, 1, cv2.LINE_AA)

        # for each person, draw keypoints
        for kpId in mappings[trackid]:
            coord =  mappings[trackid][kpId]# [x,y] in std img coords
            x = int(coord[0])
            y = int(coord[1])
            position = (x,y)       
            cv2.circle(img,position,3,color,-1) # circle radius, stroke thickness

        count += 1

    vidwriter.write(img)


    if flag == True:
        vidwriter.release()
        print('Done!')
        exit()




