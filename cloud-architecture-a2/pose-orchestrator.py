"""
This class will serve as the driver for setting up a single camera
"""
from geom import Geometry
from Subscribe import Subscribe
from regex import Regex
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from shapely.geometry import box
import shapely.ops as so
import time
import datetime
import matplotlib.pyplot as plt
import psycopg2
from psycopg2.extensions import adapt, register_adapter, AsIs
from payload.payload import Payload
from sqlops.Postgres import Database
from status.status import Status
from pose.trt import utilstrt


class Orchestrator(Geometry, Regex, Database):

	def __init__(self, region, lines, rule, commands, source):
		Geometry.__init__(self, region, lines)
		# Subscribe.__init__(self, 'topic')
		Regex.__init__(self, rule)
		Database.__init__(self)
		self.commands = commands
		self.source_id = source

	def sendtodb(self,):
		print('Completed Processing')
		print('Now Generating reports')
		database = Database()
		database.generate_report(self.commands)
		print('Reports are saved.')

	# self._do()

	def _do(self, ret):
		"""
		Iterate through the csv here; and put centroid of bounding box
		and then check if the point matches any regions

		then take that region and see if its a match;then keep
		"""

		# if self.rules == []:
		#     return

		for tup in ret:
			objectid, centroid = tup
			currentState = self.obj_state(self.create_point(centroid))
			if len(currentState) > 0:
				current_time = datetime.datetime.now()  # ending timestamp
				ret = self.match(currentState[0], objectid, current_time, centroid)
				if ret:
					self.send_message(ret, self.source_id)




payload = Payload('payload/pose.json')
print('THIS IS THE PAYLOAD BELOW')

orchestrate = Orchestrator(payload.dict['anns'], payload.dict['lines'], payload.dict['regex'], payload.commands,
						   payload.dict['videokey'])

print(payload.dict['anns'])
count = 0
for ret in utilstrt(payload.dict['anns']):
	count += 1
	orchestrate._do(ret)
orchestrate.sendtodb()

# # #



