import multiprocessing as mp
import collections
import json
import ast
import datetime
import os
import threading
import os.path
import cloudconfig

Msg = collections.namedtuple('Msg', ['event', 'args'])

class BaseProcess(mp.Process):
	"""A process backed by an internal queue for simple one-way message passing.
	"""
	def __init__(self, q, *args, **kwargs):
		super().__init__(*args, **kwargs)

		#self.queue = mp.Queue()
		self.queue = q
		#for saving data to local machine
		self.messageBuffer = {} # key=topic, val=list of payloads from a topic
		self.seenTopics = [] # array of seen topics
		self.saveJsonFlag = cloudconfig.save_json_flag # set this to true if you want to save messages to json
		self.saveJsonPeriod = 50 # save to json every N messages for a topic
		self.prevTime = datetime.datetime.now() # for keeping track of when to flush the buffer
		self.count = 0 #i want to save to the json file this many times

	def parse_message(self,dict):
		#msg = Msg(dict['sourceid'],dict)
		msg = Msg(1, dict)
		return msg

	def send(self,payload,counter,topic):
		"""
		"""
		self.queue.put((payload,topic))

	def dispatch(self, msg):
		
		if msg['type'] == 'pose':
			handler = getattr(self, 'do_2', None)
		elif msg['type'] == 'object':
			handler = getattr(self, 'do_1', None)
		else:
			handler = getattr(self, 'do_qrlight', None)
		
		if not handler:
			#here is where you keep your logic of creating another source function!!!!
			raise NotImplementedError("Process has no handler for [%s]" % event)
		handler(msg)

	#message coming from the 
	def dispatch_orchestrator(self,dict):
		self.create_orchestrator(dict)
	
	#message will be given to delete orchestrator and rows from the
	#presence table and the activity row table
	#{companyname+camID}; {companynamecamID}_{presence}
	def clear_analysis(self,dict):
		pass


	# saves a payload dict to a buffer
	# saves to file once buffer has reached N messages for a topic.
	# every t seconds, flushes the buffer and saves to json for all topics
	# INPUTS:
	#          topic (str): eg "Sensable/269"
	#          ploadDict (dict): payload to be saved in json
	#          bufferLimit (int): N
	def save2json(self,topic:str, ploadDict:dict):

		self.count += 1
		print(self.count)
		filename = topic.replace("/","-") + ".json" # "Sensable-269.json" 

		# if this is the first time we've seen this topic
		if topic not in self.seenTopics:
			self.seenTopics.append(topic)
			# create directory if no existing dir
			if not os.path.isdir('saved-json'):
				os.mkdir('saved-json')
			# delete old file 
			if os.path.isfile(filename):
				os.remove(filename)

		# append payload dict to buffer
		if(topic not in self.messageBuffer): self.messageBuffer[topic] = [ploadDict]
		else: self.messageBuffer[topic].append(ploadDict)

		# if t seconds has elapsed, flush messageBuffer for all topics (save and reset)
		deltaTime = datetime.datetime.now() - self.prevTime
		if(deltaTime.seconds > 20):
			# save messages for all topics, then reset buffer
			for topic in self.messageBuffer:
				if(len(self.messageBuffer[topic]) > 0):
					filename = topic.replace("/","-") + ".json" # eg "Sensable-269.json" 
					with open('saved-json/'+filename,'w') as outjson:
						#debug prints
						print("Time to flush buffer..Saving to {}..".format(filename))
						json.dump(self.messageBuffer[topic],outjson)
			self.prevTime = datetime.datetime.now()

		#after saving message to dict, then return
		return 
		
		# if the counter for this topic has reached a threshold, 
		# we save the buffer,
		# then reset the buffer
		# self.count += 1
		if self.count % 50 == 0: # -> we will then lose 500 messages -> 1000 frames  -> 30 seconds
			print('saving json',self.count)
			with open('saved-json/'+filename,'w') as outjson:
				# debug prints
				# print("Buffer[topic] has {} messages...Saving to {} ...".format(bufferLimit,filename))
				json.dump(self.messageBuffer[topic], outjson)	
		
		return


	def run(self):
		while True:
			msg = self.queue.get()
			# debug prints
			# print(msg)
			#if true then this message is coming from muvva
			if type(msg) == type(()):
				payload,topic = msg
				payloadDict = json.loads(payload)

				# expects topic to be "Company/123"
				if(self.saveJsonFlag):
					self.save2json(topic, payloadDict)

				for message in payloadDict:
					message['topic'] = topic
					self.dispatch(message)
				
			else:
				msg = json.loads(msg)
				#if true then the message is coming from ui 
				#need to check message id here to see what message 
				#messageId -> start_analysis
				#messageId -> clear
				if type(msg) == type({}):
					self.dispatch_orchestrator(msg)
		






