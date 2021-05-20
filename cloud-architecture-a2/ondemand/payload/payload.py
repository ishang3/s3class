import json
import numpy as np
#from models import Base

import psycopg2
from psycopg2.extensions import adapt, register_adapter, AsIs
import pandas as pd
import json
import ast
import numpy as np
import re


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
		self.dict = {
					 'human-readable-regex':{}, #mapping 
					 'table_name': '',
					 'logId' : '', 
					 }
		self.commands = {}
		self._objects = []
		self.targetSize_x = 1920
		self.targetSize_y = 1080
		self.forklift = False
		self.orch = orch
		self._ret()


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
		self.dict['shift_table_name'] = shift_table_name

	def _ret(self):
		"""

		"""

	
		self.create_table()	

	
		self.dict['cameraid'] = self.data['cameraId']
		self.dict['logId']    = self.data['cameraReportLogId']
		for entry in self.data['reports']:
			self.commands[entry['type']] = entry['command']













