from baseprocess import BaseProcess
from orchestrator import Orchestrator
from confluent_kafka import Consumer, KafkaException
from centroid.centroidtracker import CentroidTracker
import sys
import getopt
import json
import logging
from pprint import pformat  
import time
from payload.payload import Payload
from sqlops.Postgres import Database
from sqlops.pub import sendtokafka
import json
from aiokafka import AIOKafkaConsumer
import asyncio
from helpers.helper import body_labels
from helpers.helper import get_bbox
from helpers.helper import convert_kp
from helpers.helper import inverse_body_labels
from helpers.helper import filter_keypoints
from helpers.helper import get_core_centroid
from ergonomics.ergonomicsorch import Ergonomics
from idle.idleorch import Idle
from regionopresence import RegionPresence
from qr.qr_orch import QrProcessing
from lighting.light import LightProcessing
#from personpresence.personfinder import PersonFinder
from posepresence.posepresence import PoseFinder
import cloudconfig
import numpy as np
from trackervisual import write_frame


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

#1920x1080 will be the json
#enable osd to get it working
class MyProcess(BaseProcess):

	def __init__(self,q):
		BaseProcess.__init__(self,q)
		self.mappings = {}
		self.temp = []
		self.qr_counter = 0
		self.multiple_json = cloudconfig.multiple_json_flag
	
		#self.data = json.load(open('data/interval_0_2.json', "r"))
		

	def edit_analysis():
		"""
		function dedicated to making changes to the current analysis
		"""
		pass


	def remove_orchestrator(self,payload):
		"""
		This code is going to remove the orchestrator from the self.mappings
		Need to also remove all the database associated with this particular db
		NEED TO DO
		1) remove orch from self.mappings
		2) Clear tables associated with this table id
		"""
		pass

	def create_orchestrator(self,dict):
		"""
		this needs to be run in 2 ways
		1) I am subscribed to the start-analysis json
		2) Also access to their DB in determining when to add to my mappings
		"""
		
		topic = dict['iotCoreTopic']

		#need to check if orch already exists, if so, then delete and replace
		if topic in self.mappings:
			self.mappings.pop(topic, None)

		#******************************** 
		# NEED TO ADD ERROR CHECK TO RETURN IF
		#  PAYLOAD.DICT WAS NOT PROPERLY ADDED TO THE INFORMATION

		#creating the entire payload object per camera information
		payload = Payload(dict)

		print(payload.dict)

		print('*****************************************************')
		print('*****************************************************')
		print('*****************************************************')
		print('*****************************************************')
		print('*****************************************************')
		print('*****************************************************')

		orchestrate = Orchestrator(payload.dict['anns'], payload.dict['lines'],
                                           payload.dict['regex'],payload.dict['regex_type'],
                                           payload.commands, payload.dict['table_name'],
                                           payload.dict['presence_table_name'],
										   payload.dict['region_presence_state'],
										   payload.dict['presence_region_regex'],
										   payload.dict['presencekp_state'],
										   payload.dict['pose_presence_regex'],
										   payload.dict['ergonomic_state'],
										   payload.dict['ergonomic_regex'],
										   pose=True) # no pose


		pose_orchestrate = Orchestrator(payload.dict['anns'], payload.dict['lines'],
                                           payload.dict['pose_regex'],payload.dict['pose_regex_type'],
                                           payload.commands, payload.dict['table_name'],
                                           payload.dict['presence_table_name'],
										   payload.dict['region_presence_state'],
										   payload.dict['presence_region_regex'],
										   payload.dict['presencekp_state'],
										   payload.dict['pose_presence_regex'],
										   payload.dict['ergonomic_state'],
										   payload.dict['ergonomic_regex'],
										   pose=False # we want pose 
										   ) 


		self.mappings[topic] = {
								'object_state'  : payload.dict['object_state'], #need to replace with some value coming from payload
								'payload' : payload.dict,
								'prev_frameid' : None,
							  	'current_frameid' : None,
							  	'rects' : {},
								'counter' : 0,
							  	'ct': CentroidTracker(),
								'orchestrator' : orchestrate,
								#everything below will be for the handling of the pose data
								'pose_state' : payload.dict['pose_state'], #need to repalce with some value coming from payload
								'pose_prev_frameid' : None, # relative to first vid
							  	'pose_current_frameid' : None, # relative to first vid
							  	'pose_rects' : {},
								'pose_counter' : 0,
							  	'pose_ct': CentroidTracker(pose=True),
								'pose_orchestrator' : pose_orchestrate,
								#everything below will be for handling of the idle time state
								'idle_state' : payload.dict['idle_state'],
								'idle_orch'  : Idle(payload.dict['idle_regex'],payload.dict['table_name'],
                                           payload.dict['presence_table_name']) if payload.dict['idle_state'] else None,
								#everything below will be for the handling of the presence with respect to region state
								'region_presence_state': payload.dict['region_presence_state'],
								'region_presence_orch' : None, #PersonFinder(payload.dict['presence_region_regex'],payload.dict['table_name'],
                                          # payload.dict['presence_table_name']) if payload.dict['region_presence_state'] else None,
								#everything below will be for handling of the qr code logic 
								'qr_presence_state'    : payload.dict['qr_state'],
								'qr_presence_orch'	   : QrProcessing(payload.dict['qr_regex'],payload.dict['table_name'],
                                           								 payload.dict['presence_table_name']) if payload.dict['qr_state'] else None,											
								#Here it will be for the light orch and state
								'light_state' 		    : payload.dict['light_state'],
								'light_orch'  	        : LightProcessing(payload.dict['light_regex'],payload.dict['table_name'],
                                           								 payload.dict['presence_table_name']) if payload.dict['light_state'] else None

								}

		#print(self.mappings)


	def parse_data(self,message,):
		"""
		This will parse the message for the
		tracking id and the object bbox coords

		Return
		frameid, keypoint, generate bbox from keypoint, topic, timestamp
		"""
		frameid = int(message['frameid'])
		
		if self.multiple_json == True:
			uri = message['uri']
			relativeframeid = message['relativeframeid']
		else:
			uri = None
			relativeframeid = frameid

		bbox =    message['object']['bbox']
		topic = message['topic']
		objtype = message['object']['objectType']
		coords = [int(bbox['topleftx']), int(bbox['toplefty']),
				  int(bbox['bottomrightx']), int(bbox['bottomrighty'])]
		
		timestamp = message['timestamp']


		return frameid,coords,topic,objtype,timestamp,uri,relativeframeid


	def parse_pose_data(self,message):
		"""
		This will parse the message that is of the pose format
		"""
		uri = message['uri']		
		kp_list = []
		bbox_list = [] # bbox for each person
		centroid_list = [] # centroid of kp for each person. [(cx1,cy1),(cx2,cy2),...]
		frameid = int(message['frameid'])

		if self.multiple_json == True:
			relativeframeid = message['relativeframeid']
		else:
			relativeframeid = frameid

		keypoints = message['keypoints'] 
		timestamp = message['timestamp']
		topic = message['topic']
		#this will convert the pose points into a bounding box 
		#this is going through multiple sets of keypoints for more than 1 person
		for person in keypoints:
			kps = keypoints[person]
			'''
			what kps looks like:
			{'0': [321.2842102050781, 182.49850463867188], '1': [329.1330261230469, 175.37741088867188], 
			'2': [318.6502990722656, 174.97030639648438]}
			'''

			#check if the number of kps is less than cloudconfig.minKP
			if len(kps) < cloudconfig.minKP: # 4,6
				continue
			
			# get centroid of core keypoints (exclude wrist,elbow) for this person
			centroid = get_core_centroid(kps) # centroid (Cx,Cy)
			centroid_list.append(centroid) # [(cx1,cy1),(cx2,cy2),...]

			bbox = convert_kp([kps])
			kp_list.append(kps)
			bbox_list.append(bbox)
			# print(len(kps))
			
		return frameid,centroid_list, bbox_list,topic,kp_list,timestamp,uri,relativeframeid
	
	#for jbs
	def parse_qrlight_data(self,data):
		"""
		returned a parsed pose data
		"""

		timestamp = data['timestamp']
		qrlabels = data['qr']
		light = data['lights']
		topic = data['topic']

		return timestamp,qrlabels,light,topic


	#this is going to be for parsing the pose data
	def do_2(self, arg1, *args):
		"""
		do the pose processing here

		EDGE CASES
		1) need to handle case if keypoint doesnt show up in the metadata
		2) before adding it to a pose rect; need to check if it includes the keypoint
		3) to centroid tracker need to return 3 things; (coords,keypoint,objtypemapping as above right or the number)
		"""

		# print('this is hitting',arg1)
		frameid, centroid_list, coords, topic, keypoints, timestamp, source, relativeframeid = self.parse_pose_data(arg1)
		# print("source ",source, "--->", frameid, "--",timestamp)
	
		if topic not in self.mappings:
			print("Unknown topic!")
			return

		# if topic not in self.mappings or self.mappings[topic]['pose_state'] == False:
		# 	return

		# print(len(coords))
		if len(coords) == 0:
			return

		# print(len(coords),len(keypoints),'*$#$#$##$*$#*#$**$#')
		for ctd,coord,keypoint in zip(centroid_list,coords,keypoints): #this is going through each list of keypoints for each person in that frame

			#this will remove the unwanted keypoints we do not want to track. NOTE: currently broken
			# keypoint = filter_keypoints(keypoint,self.mappings[topic]['payload'])

			if len(keypoint) == 0:
				# print("kp len is 0")
				return

			# print('here')
			#checking the previous frame value from none to current
			if self.mappings[topic]['pose_prev_frameid'] == None:
				self.mappings[topic]['pose_prev_frameid'] = relativeframeid

			# update current frame
			self.mappings[topic]['pose_current_frameid'] = relativeframeid

			#handling stuff for tracker
			rects_counter = self.mappings[topic]['pose_counter']
			if(cloudconfig.KPCentroidFlag==True):
				coord = (ctd[0],ctd[1],ctd[0],ctd[1])

			self.mappings[topic]['pose_rects'][rects_counter] = (coord, keypoint)
			self.mappings[topic]['pose_counter'] += 1

			#once new frame is detected then send begin to process previous's frames data
			if self.mappings[topic]['pose_current_frameid']  != self.mappings[topic]['pose_prev_frameid']:
				
				# print(self.mappings[topic]['pose_prev_frameid'],self.mappings[topic]['pose_current_frameid'] )			
				rects = self.mappings[topic]['pose_rects']
				ct = self.mappings[topic]['pose_ct']

				# return values from centroid tracker
				# if current frame - prev frame >= 2, call ct.update (diff-1) times with empty rects
				frameDiff = self.mappings[topic]['pose_current_frameid'] - self.mappings[topic]['pose_prev_frameid']
				for i in range(frameDiff-1):
					objects,last_location, mappings, bbox_mappings = ct.update({})

				objects, last_location, mappings, bbox_mappings  = ct.update(rects)

				if int(frameid) > 20000:
					write_frame(bbox_mappings,mappings,frameid,True)
				else:
					write_frame(bbox_mappings,mappings,frameid,False)




				# print('Below is the bbox mappings************')
				# print('**********************************')
				# print(bbox_mappings)

				# print('mappings')
				# print(mappings)

				# print('objects')
				# print(objects)
				#comment for testing feature for tracker visualization
		

				frame_yield = {}
				new_mappings = {} # this will be a mapping between 1a - > keypoint number
				for id in mappings:

					#keypoints is the first item on the list
					keypoints_map = mappings[id]
					#iterating through the keypoints
					for kp_name in keypoints_map:

						kp_coords = keypoints_map[str(kp_name)]

						bodylabel = body_labels[int(kp_name)]
					
						#yields the tracking id, respective body label, and centrid or keypoint
						#print(id, bodylabel, kp_coords)
						new_id = str(id)+bodylabel
						new_mappings[new_id] = int(kp_name)
						# 1a = (100,100) 
						# id 1; and keypoint a; should be used to work
						frame_yield[new_id] = kp_coords


				#need to reset the counter and rects as well
				self.mappings[topic]['pose_counter'] = 0
				self.mappings[topic]['pose_rects'] = {}
			
				#reset the previous frame id to currentone
				self.mappings[topic]['pose_prev_frameid'] = self.mappings[topic]['pose_current_frameid']

				total = []
				for (objectID, centroid) in frame_yield.items():
					kpname_s = list(objectID)[-1] #1a - >  [1,'a'] -> 'a'
					#print(kpname_s,objectID)
					nickname_s = inverse_body_labels[kpname_s]
					try:
						#print(centroid,objectID,keypoint[nickname_s],int(self.mappings[topic]['pose_prev_frameid']))
						#total.append((objectID,keypoint[nickname_s]))
						total.append((objectID,centroid))
					except:
						pass

			
				ret = total, new_mappings, timestamp
				# print("calling orchestrator in engine2")
				#self.mappings[topic]['pose_orchestrator']._do(ret,int(frameid),source,relativeframeid)

	
	#this is going to be for processing the object detectiond ata
	def do_1(self, arg1, *args):
		"""
		processing happening here
		"""
		
		if(cloudconfig.engine2_do1_flag==False):
			return
		
		frameid, coords, topic, objtype, timestamp, source, relativeframeid = self.parse_data(arg1)

		#handles case for no orch defined or no object orch defined for a single topic
		if topic not in self.mappings:
			return 

		#integrate this line later
		# self.mappings[topic]['object_state'] == False:
		# 	return

		#checking the previous frame value from none to current
		if self.mappings[topic]['prev_frameid'] == None:
			self.mappings[topic]['prev_frameid'] = relativeframeid

		#updating info
		self.mappings[topic]['current_frameid'] = relativeframeid

		#handling stuff for tracker
		rects_counter = self.mappings[topic]['counter']
		self.mappings[topic]['rects'][rects_counter] = (coords, objtype)
		self.mappings[topic]['counter'] += 1

		#once new frame is detected then send begin to process previous's frames data
		if self.mappings[topic]['current_frameid']  != self.mappings[topic]['prev_frameid']:
			
			rects = self.mappings[topic]['rects']
			ct = self.mappings[topic]['ct']

			# return values from centroid tracker
			# if current frame - prev frame >= 2, call ct.update (diff-1) times with empty rects
			frameDiff = self.mappings[topic]['pose_current_frameid'] - self.mappings[topic]['pose_prev_frameid']
			for i in range(frameDiff-1):
				objects,last_location, mappings = ct.update({})
			objects, last_location, mappings = ct.update(rects)

			#need to reset the counter and rects as well
			self.mappings[topic]['counter'] = 0
			self.mappings[topic]['rects'] = {}
			
			#reset the previous frame id to currentone
			self.mappings[topic]['prev_frameid'] = self.mappings[topic]['current_frameid']

			total = []
			for (objectID, centroid) in objects.items():
				total.append((objectID,centroid))

			ret = total, mappings, timestamp
			
			#if idle orch exists, send data there
			if self.mappings[topic]['idle_state'] == True:
				self.mappings[topic]['idle_orch'].process_total(total,mappings,timestamp)

			#send data to orchestrator
			self.mappings[topic]['orchestrator']._do(ret,int(self.mappings[topic]['prev_frameid']),source,relativeframeid)
	
	#this is going to be for qr code processing
	def do_qrlight(self, arg1, *args):
		"""
		This can be the qr code / light proocessor

		Need to create some qr code object type

		"""
		
		
		timestamp, qrlabels, light, topic = self.parse_qrlight_data(arg1)

		#if topic not in mappings then return
		if topic not in self.mappings:
			return

		# print(qrlabels)
		# print(light)
		# print(topic)
		# return

		#send data accordingly to their respective classes
		if self.mappings[topic]['qr_presence_state'] == True:
			self.qr_counter += 1
			self.mappings[topic]['qr_presence_orch'].process_input_data(qrlabels,timestamp,topic)
		
		if self.mappings[topic]['light_state'] == True:
			self.mappings[topic]['light_orch'].process_light_data(light,timestamp)








#good links here: https://stackoverflow.com/questions/11515944/how-to-use-multiprocessing-queue-in-python