import json
import numpy as np
#from models import Base
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, inspect, TIMESTAMP,Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence

import psycopg2
from psycopg2.extensions import adapt, register_adapter, AsIs
import pandas as pd
import ast
import re
from sqlops.createpresencetable import Partition
from helpers.helper import bodypart_to_letter
import cloudconfig

class Payload:
	"""
	This will parse the json and return the
	necessary objects for the orchestrator

	Need to return the bottom:
	1) Dictionary of Regions
	2) Corresponding Regex
	3) Source Id (video key)
	"""
	def __init__(self, payload,orch=True):
		self.data = payload  #json.load(open(payload, "r")) #    #json.loads(payload) # #payload #json.load(open(payload, "r"))
		self.dict = {'regex':[],
					 'regex_type' : {},
					 'pose_regex' : [], #saves a list of all the regex assoicated with pose
					 'pose_regex_type' : {}, #creates a mapping between the keypoint and rule {pa: []}
					 'ergonomic_regex' : {},
					 'presence_region_regex':{}, #mapping for understanding presence in specific regions
					 'pose_presence_regex' : {},
					 'qr_regex' : [],
					 'light_regex' : [],
					 'idle_regex': [], 
					 'human-readable-regex':{}, #mapping
					 'table_name': '',
					 'logId' : '', 
					 'object_state': False,
					 'pose_state': False, #using a keypoint as an object
					 'ergonomic_state' : False,  #self explantor
					 'idle_state' : False, #understanding if an object is just staying stationary
					 'region_presence_state' : False, #for detecting presence wrt to each region
					 'qr_state' : False,
					 'light_state' :False,
					 'presencekp_state' : False,
					 'presencekpand_state' : False
					 }
		DATABASE_URI = 'postgresql://postgres:sensable01@sensable-2.cesrxorctwi1.us-east-2.rds.amazonaws.com:5432/demo'
		self.engine = create_engine(DATABASE_URI)
		self.Base = declarative_base()
		self.commands = {}
		self._objects = []
		self.targetSize_x = cloudconfig.infer_wd
		self.targetSize_y = cloudconfig.infer_ht
		self.forklift = False
		self.orch = orch
		self._ret()

	def fix_bb(self,boxes):
		ret = {}
		for ann in boxes:
			if ann['type'] != 'rect':
				continue

			if len(ann['analysis']) > 0:
				self._objects.append(ann['analysis'][0]['type'])

			image_width = ann['image_width']
			image_height = ann['image_height']
			
			x_scale = self.targetSize_x / image_width
			y_scale = self.targetSize_y / image_height

			xmin = int(np.round(ann['xmin'] * x_scale))
			ymin = int(np.round(ann['ymin'] * y_scale))
			xmax = int(np.round(ann['xmax'] * x_scale))
			ymax = int(np.round(ann['ymax'] * y_scale))

			ret[ann['label']] =  (xmin, ymin, xmax, ymax)

			print(ann['label'],xmin,ymax,xmax-xmin,ymax-ymin,'****************')

		return ret

	def fix_lines(self,anns):
		ret = {}
		for ann in anns:
			if ann['type'] != 'line':
				continue

			#parsing all the classes that need to be analyzed
			random = [x['type'] for x in ann['analysis']]
			if random == ['pallets']:
				self._objects.extend(['pallet'])
			self._objects.extend(random)

			image_width = ann['image_width']
			image_height = ann['image_height']

			x_scale = self.targetSize_x / image_width
			y_scale = self.targetSize_y / image_height

			x1 = int(np.round(ann['xmin'] * x_scale))
			y1 = int(np.round(ann['ymin'] * y_scale))
			x2 = int(np.round(ann['xmax'] * x_scale))
			y2 = int(np.round(ann['ymax'] * y_scale))

			print('THIS IS THE LINE CODE BEING RUN ')
			print('****************** - comment out later')

			print(x1,y1,x2,y2,'***************************************')
			print(image_width,'this is the image width coming from the ui',image_height,'this is the image height coming from the ui')

			print('****************** - comment out later')

			label = ann['label']
			ret[label] = {}

			#setting the coords
			ret[label]['coords'] = (x1, y1, x2, y2)

			#for data coming ui
			s1x,s1y = ann['S1']['px1'],ann['S1']['py1']
			s2x, s2y = ann['S2']['px2'], ann['S2']['py2']


			s1x = int(np.round(s1x * x_scale))
			s1y = int(np.round(s1y * y_scale))
			s2x = int(np.round(s2x * x_scale))
			s2y = int(np.round(s2y * y_scale))

			ret[label]['S1'] = [s1x,s1y]
			ret[label]['S2'] = [s2x,s2y]

			#SHOULD BE COMMENTED OUT when not coming from ui
			# ret['S1']  = ann['S1']
			# ret['S2']  = ann['S2']

		print(ret)
		return ret

	def create_regex(self,regex):
		try:
			for ann in regex:
				# print(ann)
				#this will create a mapping between the human readable activity and the regex
				self.dict['human-readable-regex'][ann['regex'].split(':')[2]] = ann['activity_name']

				# creating a regex for presence with respect to a qr label
				if ann['regex'].split(':')[0].lower() == 'light':

					#this will activate that this payload contains a pose object state to true
					self.dict['light_state'] = True

					#getting the value of third element which will correspond with station number
					value = ann['regex'].split(':')[2]
					key = 'light_regex'
					if key not in self.dict:
						self.dict[key] = value

				# creating a regex for presence with respect to a qr label
				if ann['regex'].split(':')[0].lower() == 'qr':

					#this will activate that this payload contains a pose object state to true
					self.dict['qr_state'] = True

					#getting the value of third element which will correspond with station number
					value = ann['regex'].split(':')[2]
					key = 'qr_regex'
					if key not in self.dict:
						self.dict[key] = value
					
				# creating a regex for presence with respect to a region
				if ann['regex'].split(':')[0].lower() == 'rp':

					#this will activate that this payload contains a pose object state to true
					print("Found grammar for object detection region presence")
					self.dict['region_presence_state'] = True
 
					#creates a mapping between object type and different regions
					#p:R0,R1
					key = 'presence_region_regex'
					object_type = ann['regex'].split(':')[1]
					if object_type not in self.dict['presence_region_regex']:
						#mapping between object type and metadata
						# p: tuple(['R0','R1'],'R0,R1')
						self.dict['presence_region_regex'][object_type] = [ann['regex'].split(':')[2]]
					else:
						self.dict['presence_region_regex'][object_type].append(ann['regex'].split(':')[2])


					
				# creating a regex for understanding standing time 
				if ann['regex'].split(':')[0].lower() == 'idle':

					#this will activate that this payload contains a pose object state to true
					self.dict['idle_state'] = True
 
					key = 'idle_regex'
					if key not in self.dict:
						self.dict['idle_regex'] = [ann['regex'].split(':')[1]]
					else:
						self.dict['idle_regex'].append(ann['regex'].split(':')[1])

				#this is now going to parse for the ergonomic keypoint
				# (tuple of 3 coordinate) : angle wanted
				if ann['regex'].split(':')[0] == 'pose' or ann['regex'].split(':')[0] == 'Pose':
					print("found an ergo regex")
					#this will activate that this payload contains a pose object state to true
					self.dict['ergonomic_state'] = True
 
					# parse the regex
					threeKP = ann['regex'].split(':')[1].split(',') # array of 3 keypoints. eg: ['pf','pg','pb']
					angleCompare = ann['regex'].split(':')[2].split(',')[0] # either '<' or '>'
					angleThresh = float(ann['regex'].split(':')[2].split(',')[1]) # float. threshold for angle in degrees. eg: 70
					tmin = float(ann['regex'].split(':')[3]) # float. minimum duration in seconds. eg: 0.5
					# 'ergonomic_regex' maps to 'threeKP' maps to [angleCompare, angleThresh, tmin]
					self.dict['ergonomic_regex'][tuple(threeKP)] = [angleCompare,angleThresh,tmin]


				# this is now going to parse for the keypoint presence in certain region
				# See "current grammar" google doc for regex syntax
				# example:
				# presencekp:pq,pa:region=R0:tmin=0.1:subsetcount=2
				if ann['regex'].split(':')[0].lower() == 'presencekp':
					print("Found a regex for presencekp!")
					#this will activate that this payload contains a pose object state to true
					self.dict['presencekp_state'] = True
 
					object_type = ann['regex'].split(':')[1].split(',') # -> ['pa','pb','pd']
					region =  (ann['regex'].split(':')[2])[7:] # eg: R7
					timemin = float((ann['regex'].split(':')[-2])[5:]) # eg: 0.1
					subsetcount = int((ann['regex'].split(':')[-1])[12:]) # eg: 2
					if region not in self.dict['pose_presence_regex']:
						#mapping between object type and metadata
						# R0 ['pa','pb']
						# R1 : ['pa','pb','pc']
						self.dict['pose_presence_regex'][region] = [object_type,timemin,subsetcount]
				
				#pose presence AND; multiple points
				#presencekpand:R1,R3:{name of activity}
				if ann['regex'].split(':')[0].lower() == 'presencekpand':
					print('Found a regex for presencekpand')



				#this is now going to parse for the keypoint used
				# kp:{insert specific keypoint}:{regex}
				if ann['regex'].split(':')[0].lower() == 'kp':


					#{Kp}:{pj,pk}:{R1R2} - Example

					#this will activate that this payload contains a pose object state to true
					self.dict['pose_state'] = True
 
					key = 'pose_regex'
					if key not in self.dict:
						self.dict['pose_regex'] = [ann['regex'].split(':')[2]]
					else:
						self.dict['pose_regex'].append(ann['regex'].split(':')[2])

					# mapping between {'pa' : [ROR1,R1R2], 'pb' : []} - CURRENT
					#new mapping will be { ('pa','pb','pc') : ['R0R1'],
					#  						'pa'   : [R0R1}
					#this is for the regex to build mappings between the regex and object
					key = 'pose_regex_type'
					object_type = ann['regex'].split(':')[1].split(',') # -> ['pa','pb','pd'] or ['pa']
					
					if len(object_type) == 1:
						# ['pa'] -> 'pa'
						object_type = object_type[0]
			
					else:
						# ['pa','pb'] -> ('pa','pb')
						object_type = tuple(object_type)

					if object_type not in self.dict['pose_regex_type']:
						self.dict['pose_regex_type'][object_type] = [ann['regex'].split(':')[2]]
					else:
						self.dict['pose_regex_type'][object_type].append(ann['regex'].split(':')[2])


				#this is now going to parse the regex used
				if ann['regex'].split(':')[0] == 'o' or ann['regex'].split(':')[0] == 'O':

					#this will activate that this payload contains object state to true
					self.dict['object_state'] = True

					key = 'regex'
					if key not in self.dict:
						self.dict['regex'] = [ann['regex'].split(':')[2]]
					else:
						self.dict['regex'].append(ann['regex'].split(':')[2])

					#this is for the regex to build mappings between the regex and object
					key = 'regex_type'
					object_type = ann['regex'].split(':')[1]
					if object_type not in self.dict['regex_type']:
						self.dict['regex_type'][object_type] = [ann['regex'].split(':')[2]]
					else:
						self.dict['regex_type'][object_type].append(ann['regex'].split(':')[2])

		except Exception as e:
			print(e)
		


	'''
	Given json from wizard tool, set self.dict accordingly

		THIS IS WHAT the input (wizardJson) LOOKS LIKE

			{'activity_name': 'act name here', 
			 'activityJson': {'activityName': 'act name here', 'motion': '', 'ergonomics': '', 'presence': 'R1', 
				                  'advancedActivityType': '', 'objectTypelist': ['Left_shoulder', 'Right_shoulder'], 
								   'minimumKeypointCount': '1', 'minimumTime': '0.25', 'maximumTime': '5.0', 'angle': ''}
			},

			{'activity_name': 'act name 2', 
			 'activityJson': {'activityName': 'act name 2', 'motion': '', 'ergonomics': '', 'presence': 'R1', 
				                  'advancedActivityType': '', 'objectTypelist': ['Left_shoulder', 'Right_shoulder'], 
								   'minimumKeypointCount': '1', 'minimumTime': '0.25', 'maximumTime': '5.0', 'angle': ''}
			}
	'''
	def create_regex_from_wizard(self,wizardJson):
		try:
			for ann in wizardJson:
				'''
				THIS IS WHAT ann LOOKS LIKE
				{'activity_name': 'act name here', 
				 'activityJson': {'activityName': 'act name here', 'motion': '', 'ergonomics': '', 'presence': 'R1', 
				                  'advancedActivityType': '', 'objectTypelist': ['Left_shoulder', 'Right_shoulder'], 
								   'minimumKeypointCount': '1', 'minimumTime': '0.25', 'maximumTime': '5.0', 'angle': ''}
				}
				'''
				# this will create a mapping between the human readable activity and the regex
				# might need to add line below
				# self.dict['human-readable-regex'][ann['regex'].split(':')[2]] = ann['activity_name']

				# we want presence (either keypoint or person)
				if 'presence' in ann['activityJson'] and ann['activityJson']['presence'] != '':
					if(ann['activityJson']['objectTypelist']==['Person']):
						region = ann['activityJson']['presence'] # eg: 'R1'
						
						#this will activate that this payload contains a pose object state to true
						print("Found rule for person b.box region presence "+region)
						self.dict['region_presence_state'] = True
	
						#creates a mapping between object type and different regions
						#p:R0,R1
						key = 'presence_region_regex'
						object_type = 'person'
						if object_type not in self.dict['presence_region_regex']:
							#mapping between object type and metadata
							# p: tuple(['R0','R1'],'R0,R1')
							self.dict['presence_region_regex'][object_type] = [region]
						else:
							self.dict['presence_region_regex'][object_type].append(region)

					else:
						# kp presence in a region					
						self.dict['presencekp_state'] = True
						region = ann['activityJson']['presence'] # eg: 'R7'
						tmp = ann['activityJson']['objectTypelist']
						print("Found rule for keypoint presence!" + str(tmp)+'region: '+region)
						
						object_type = [] # to be ['pa','pb','pd']
						for bodypart in tmp:
							# convert from 'Nose' to 'pa' etc
							object_type.append(bodypart_to_letter[bodypart])

						if(ann['activityJson']['minimumTime']!=''):
							timemin = float(ann['activityJson']['minimumTime']) # eg: 0.1
						else:
							timemin = 0.0
						
						subsetcount = int(ann['activityJson']['minimumKeypointCount']) # eg: 2
						if region not in self.dict['pose_presence_regex']:
							#mapping between object type and metadata
							# R0 ['pa','pb']
							# R1 : ['pa','pb','pc']
							self.dict['pose_presence_regex'][region] = [object_type,timemin,subsetcount]

				# kp or person motion	
				elif 'motion' in ann['activityJson'] and ann['activityJson']['motion'] != '':					
					if(ann['activityJson']['objectTypelist']==['Person']):						
						# person bounding box motion
						motion = ann['activityJson']['motion'] # 'R1R2'
						print("Found a rule for person b.box motion! "+motion)
						
						#this will activate that this payload contains object state to true
						self.dict['object_state'] = True
						key = 'regex'
						
						if key not in self.dict:
							self.dict['regex'] = [motion]
						else:
							self.dict['regex'].append(motion)
						
						#this is for the regex to build mappings between the regex and object
						key = 'regex_type'
						object_type = 'p'
						if object_type not in self.dict['regex_type']:
							self.dict['regex_type'][object_type] = [motion]
						else:
							self.dict['regex_type'][object_type].append(motion)

					else:
						# kp motion
						motion = ann['activityJson']['motion'] # 'R1R2'
						print("Found a rule for kp motion! "+motion+" "+str(ann['activityJson']['objectTypelist']))
						#this will activate that this payload contains a pose object state to true
						self.dict['pose_state'] = True
						key = 'pose_regex'
						if key not in self.dict:
							self.dict['pose_regex'] = [motion]
						else:
							self.dict['pose_regex'].append(motion)
						# mapping between {'pa' : [ROR1,R1R2], 'pb' : []} - CURRENT
						#new mapping will be { ('pa','pb','pc') : ['R0R1'],
						#  						'pa'   : [R0R1}
						#this is for the regex to build mappings between the regex and object
						key = 'pose_regex_type'
						tmp = ann['activityJson']['objectTypelist']
						object_type = [] # to be ['pa','pb','pd'] or ['pa']
						for bodypart in tmp:
							object_type.append(bodypart_to_letter[bodypart])						
						if len(object_type) == 1:
							# ['pa'] -> 'pa'
							object_type = object_type[0]
						else:
							# ['pa','pb'] -> ('pa','pb')
							object_type = tuple(object_type)
						if object_type not in self.dict['pose_regex_type']:
							self.dict['pose_regex_type'][object_type] = [motion]
						else:
							self.dict['pose_regex_type'][object_type].append(motion)


				# ergonomics
				# (tuple of 3 coordinate) : angle wanted
				elif 'ergonomics' in ann['activityJson'] and ann['activityJson']['ergonomics'] != '':
					print("found an ergo rule")
					#this will activate that this payload contains a pose object state to true
					self.dict['ergonomic_state'] = True
 
					# parse the regex
					tmp = ann['activityJson']['objectTypelist']
					threeKP = [] # array of 3 keypoints. eg: ['pf','pg','pb']
					for bodypart in tmp:
						threeKP.append(bodypart_to_letter[bodypart])

					angleCompare = ann['activityJson']['angle'][0] # either '<' or '>'
					angleThresh = float(ann['activityJson']['angle'][1:]) # float. threshold for angle in degrees. eg: 70
					if(ann['activityJson']['minimumTime']!=''):
						tmin = float(ann['activityJson']['minimumTime']) # float. minimum duration in seconds. eg: 0.5
					else:
						tmin = 0.0
					# 'ergonomic_regex' maps to 'threeKP' maps to [angleCompare, angleThresh, tmin]
					self.dict['ergonomic_regex'][tuple(threeKP)] = [angleCompare,angleThresh,tmin]

				# using manual rule input
				elif('advancedActivityType' in ann['activityJson'] and len(ann['activityJson']['advancedActivityType']) > 0):
					print("Found a manual rule!")
					
					tmpdict = {} # for feeding into old function
					tmpdict['activity_name'] = ann['activityJson']['activityName']
					tmpdict['regex'] = ann['activityJson']['advancedActivityType'] # "presencekp:pf,pg:region=R7:tmin=0.5:subsetcount=1"
					self.create_regex([tmpdict]) 

				else:
					print("Invalid rule from wizard tool!")


		except Exception as e:
			print(e)


	#need to move to a helper function
	def TableCreator(self,activitytable,presencetable):
		class ActivityTable(self.Base):
			__tablename__ = activitytable
			uniquerowid = Column(Integer,
								 Sequence(f'{activitytable}_id_seq', start=1, increment=1),
								 primary_key=True)
			trackid = Column(String)
			match = Column(String)
			#object_value = Column(String) #new addition for object_value; 
			starting = Column(TIMESTAMP(timezone=False))
			totaltime= Column(Float)
			ending = Column(TIMESTAMP(timezone=False))
			#### adding columns for multiple json usecase
			relativestarting = Column(TIMESTAMP(timezone=False))
			relativending = Column(TIMESTAMP(timezone=False))
			endingsource = Column(String) #ending source for timestamp
			startingsource = Column(String) #starting source for timestamp

		return ActivityTable

	def create_table(self,):
		"""
		This will use the company name and camera id to create a table
		"""
		# sensable_1
		activity_table_name = self.data['companyName'].lower() + '_' + str(self.data['cameraId'])
		#sensable_1_presence  
		presence_table_name = self.data['companyName'].lower() + '_' + str(self.data['cameraId']) + '_' + 'presence'
		#sensable_1_shift
		shift_table_name = self.data['companyName'].lower() + '_' + str(self.data['cameraId']) + '_' + 'shift'

		#sett
		self.dict['table_name'] = activity_table_name
		self.dict['presence_table_name'] = presence_table_name
		ins = inspect(self.engine).get_table_names()
		#if table does not exist in db
		if activity_table_name not in ins or presence_table_name not in ins:
			if self.orch:
				activitytable = self.TableCreator(activity_table_name,presence_table_name)
				#p = Partition(tablename=presence_table_name,shiftname=shift_table_name)
				# #this is used to create a table in the respective database
			self.Base.metadata.create_all(self.engine)


	def _ret(self):

		print('CREATING TABLES******')
		self.create_table()	

		print(self.data)

		# printing wizard tool stuff
		# print("$$$$$$$$$")
		# print(self.data['activity'])
		if(cloudconfig.wizard_flag==True):
			# using wizard tool
			self.create_regex_from_wizard(self.data['activity'])
		else:
			# using grammar
			self.create_regex(self.data['activity'])

		self.dict['videobucket'] = self.data['videobucket']
		self.dict['videokey'] = self.data['videokey']
		self.dict['anns'] = self.fix_bb(self.data['annotations'])
		self.dict['lines'] = self.fix_lines(self.data['annotations'])
		self.dict['cameraid'] = self.data['cameraId']
		self.dict['logId']    = self.data['cameraReportLogId']
		if(self.data['reports'] != None):		
			for entry in self.data['reports']:
				self.commands[entry['type']] = entry['command']













