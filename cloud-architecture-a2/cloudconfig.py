# iot core params
# company = "JBS" # company name, for topic. Nestle, JBS, Sensable
# topicrange = [90,105] # cam id, for topic
# client_id = "ishan2" # client id for iot core
# # kafka params
# kafka_server = "prod" # prod or dev
# kafka_dev_ip = "3.22.100.122:9092"
# kafka_prod_ip = "13.59.52.241:9092"
# infer_wd = 1920 # width of video used for inference
# infer_ht = 1080 # height of video used for inference
# infer_fps = 29.97 # fps of video used for inference
# save_json_flag = False # True if we want to save the inference json
# ondemand_flag = False # True if we want to run ondemand process for summary reports
# tracker_hysteresis = 60 # the number of maximum consecutive frames a given object is allowed to be marked as "disappeared" until we need to deregister the object from tracking
# posepresence_hysteresis = 60
# hardcode_trackid_flag = False
# ergo_hysteresis = 60


# define global variables here
# usage:
#   in the module where you want to use a global variable,
#   import cloudconfig
#   print(cloudconfig.company)
#
# note:
#   avoid using "from cloudconfig import x" because this will clutter
#   the importer's namespace
#   instead, use "import cloudconfig"
# iot core params
# beta testing the wizard tool in dev
wizard_flag = True # true if we want to use the wizard tool (currently only in dev)

# flag for using centroid of kps (except wrists and elbows) instead of centroid of bbox
KPCentroidFlag = True

company = "debug" # company name, for topic. Nestle, JBS, Sensable
cam_id = "121" # for read_manual_json topic

topicrange = [1000,1080] # cam id, for topic
client_id = "ishan2" # client id for iot core ishan2 for clud
# kafka params
kafka_server = "prod" # prod or dev
kafka_dev_ip = "3.22.100.122:9092"
kafka_prod_ip = "13.59.52.241:9092"

infer_wd = 640 #width of video used for inference
infer_ht = 480 #height of video used for inference
infer_fps = 29.97 #fps of video used for inference

save_json_flag = False # True if we want to save the inference json
ondemand_flag = False # True if we want to run ondemand process for summary reports
multiple_json_flag = False
tracker_hysteresis = 10 # the number of maximum consecutive frames a given object is allowed to be marked as "disappeared" until we need to deregister the object from tracking
posepresence_hysteresis = 10
ergo_hysteresis = 10

read_manual_json_flag = True # true if we want to read json manually instead of through IOT core
manual_json_path = '/Users/ishan/Documents/POCs/jbs/trackertest/GL030039_c2.json'

minKP = 4 # in engine2.parse_pose_data, filters out detections with less than this no. of keypoints

consumer_group_id = "sr2"
engine2_do1_flag = False # true if we want to run engine2.do_1 (object detection)
# hard coded trackid params
hardcode_trackid_flag = True # true if we want to hard code track ids in orchestrator
region_to_trackid_hardcode = {
            'R1' : 1,
            'R2' : 1,
            'R3' : 1,
            'Cutter' : 1
        }
