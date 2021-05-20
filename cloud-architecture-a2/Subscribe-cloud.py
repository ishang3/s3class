from __future__ import absolute_import
from __future__ import print_function
import argparse
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
from uuid import uuid4
import boto3
import json
from decimal import *
import queue
import threading
from confluent_kafka import Consumer, KafkaException
from  engine2 import MyProcess
from aiokafka import AIOKafkaConsumer
import asyncio
from multiprocessing import Process, Queue
import random
from reporting.reporting_engine import Reporting
from reporting.refresh import ReportingRefresh
from cronjob.runcronjob import CronJob
from sqlops.delete_test import Clear
from ondemand.ondemandconsumer import Ondemand
from sqlops.drop import Drop
import requests
import json
import os
import cloudconfig

class ManualJsonRead():
	def __init__(self,q):
		self.process = MyProcess(q)
		self.send_data()

	def send_data(self,):		
		topic = cloudconfig.company+'/'+cloudconfig.cam_id
		print('reading file')
		time.sleep(20)
		data = json.load(open(cloudconfig.manual_json_path, "r"))
		for message in data:
			message = json.dumps(message)
			self.process.send(message,0,topic)

class Subscribe():
	def __init__(self,q, topicrange,clientId,company):
		self.config_clientId = clientId # client id to be used, from config file
		self.config_topic_range = topicrange # topic to be used, from config file
		self.company = company # company to be used, from config file
		self.mqtt_connection = None
		self.process = MyProcess(q)
		self._initialize()
		self.counter = 0
		self.q = q
		self.subscribe()		
			
	# Callback when connection is accidentally lost.
	def on_connection_interrupted(self,connection, error, **kwargs):
		print("Connection interrupted. error: {}".format(error))

	# Callback when an interrupted connection is re-established.
	def on_connection_resumed(self, connection, return_code, session_present, **kwargs):
		print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

		if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
			print("Session did not persist. Resubscribing to existing topics...")
			resubscribe_future, _ = connection.resubscribe_existing_topics()

			# Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
			# evaluate result with a callback instead.
			resubscribe_future.add_done_callback(self.on_resubscribe_complete)

	def on_resubscribe_complete(self, resubscribe_future):
		resubscribe_results = resubscribe_future.result()
		print("Resubscribe results: {}".format(resubscribe_results))

		for topic, qos in resubscribe_results['topics']:
			if qos is None:
				sys.exit("Server rejected resubscribe to topic: {}".format(topic))

	def _initialize(self,):
		# Spin up resources
		self.process.start()
		event_loop_group = io.EventLoopGroup(2)
		host_resolver = io.DefaultHostResolver(event_loop_group)
		client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

		self.mqtt_connection = mqtt_connection_builder.mtls_from_path(
			endpoint="a3uxhx2wvbys89-ats.iot.us-east-2.amazonaws.com",
			cert_filepath="key_files/2844a6fa72-cert.pem",
			pri_key_filepath="key_files/2844a6fa72-private.pem.key",
			client_bootstrap=client_bootstrap,
			ca_filepath="key_files/root-CA.crt",
			on_connection_interrupted=self.on_connection_interrupted,
			on_connection_resumed=self.on_connection_resumed,
			client_id=self.config_clientId,
			clean_session=False,
			keep_alive_secs=6)

		print("Connecting to {} with client ID '{}'...".format(
			"endpoint", "muvva mac"))

		connect_future = self.mqtt_connection.connect()

		# Future.result() waits until a result is available
		connect_future.result()
		print("Connected!")

	def _callback1(self,topic, payload, **kwargs):
		#self.counter += 1
		#this will send the payload and topic
		#based off the topic then we can make the filteration as needed
		#self.q.put(payload)
		
		#print(self.counter,'This is the counter')
		
		# print(topic)
		# print(payload,'****************************')
		self.process.send(payload,self.counter,topic)

	def subscribe(self,):

		print('subscribed!!!!!!!')
		received_all_event = threading.Event()
		# self.mqtt_connection.subscribe(
		# 		topic=f'Ulva2/263',
		# 		qos=mqtt.QoS.AT_MOST_ONCE,
		# 		callback=self._callback1)

		for val in range(self.config_topic_range[0],self.config_topic_range[1] + 1):
			self.mqtt_connection.subscribe(
				topic=f'{self.company}/{val}',
				qos=mqtt.QoS.AT_LEAST_ONCE,
				callback=self._callback1)
		
		'''
		for val in range(50,300):
			self.mqtt_connection.subscribe(
				topic=f'Sensable/{val}',
				qos=mqtt.QoS.AT_MOST_ONCE,
				callback=self._callback1)
		'''		
		print('subscribed!!!!!!!')
		received_all_event.wait()

class KafkaSubscribe():
	def __init__(self,q,db_q,company,server,devIp,prodIp):
		self.companyname = company # company name from config
		self.config_server = server # server type from config. 'dev' or 'prod'
		self.config_devIp = devIp # kafka dev ip from config
		self.config_prodIp = prodIp # kafka prod ip from config
		self.deltable = Clear() 
		self.drop = Drop()
		self.loop = asyncio.get_event_loop()
		self.q = q
		self.db_q = db_q
		self.loop.run_until_complete(self.consume())
		
	async def consume(self,):
		# check whether to use dev or prod
		if(self.config_server=='dev'):
			ip = self.config_devIp
		elif(self.config_server=='prod'):
			ip = self.config_prodIp
		else:
			print("Invalid server type!")
			os._exit(0)

		#topic = 'start-analysis' # change to bottom after UI has made the change
		topic = 'start-analysis-' + self.companyname
			
		consumer = AIOKafkaConsumer(
			topic,
    		loop=self.loop, bootstrap_servers=ip,#os.environ.get('brokerurl'), #dev 3.22.100.122:9092 #prod 13.59.52.241:9092
			group_id=cloudconfig.consumer_group_id) # string
		# Get cluster layout and join group `my-group`
		await consumer.start()
		try:
			# Consume messages
			async for msg in consumer:
				data = json.loads(msg.value)
				print(data)
				#this is logic for clear analysis - should be moved elsewhere
				if data['messageId'] == 'clear_analysis':
					print(data)
					self.deltable.generate_names(data['params']['cameraId'],data['params']['iotCoreTopic'])
				#message id to drop all these tables
				elif data['messageId'] == 'delete_table':
					self.drop.generate_tables(data['params']['cameraId'],data['params']['iotCoreTopic'])
				else:
					#check if company name matches this orchestrator
					self.q.put(msg.value)
					self.db_q.put(msg.value)
			
		finally:
			#Will leave consumer group; perform autocommit if enabled.
			await consumer.stop()


class RequestData():
    def __init__(self,q,companyname):
        self.q = q 
        self.response = requests.get("http://prod.getsensable.com/api/v1/system-token/1")
        self.token = self.response.json()['token']
        self.companyname = companyname
        self.get_setups()

    def get_setups(self,):
        headers = {
            "Authorization" : f"Bearer {self.token}"
        }
        response2 = requests.get(f"http://prod.getsensable.com/api/v1/company/name/{self.companyname}",headers=headers)
        #print(response2.json())

        for res in response2.json():
            print('***************')
            # res = str(res)
            # print(res)
			#need to uncomment and then also fix the parser in base process
            print(res)
            self.q.put(json.dumps(res))
    
if __name__ == '__main__':
	orch_q = Queue()
	database_q = Queue()
	p = Process(target=Subscribe, args=(orch_q,cloudconfig.topicrange,cloudconfig.client_id,cloudconfig.company))
	p2 = Process(target=KafkaSubscribe,args=(orch_q,database_q,cloudconfig.company,cloudconfig.kafka_server,cloudconfig.kafka_dev_ip,cloudconfig.kafka_prod_ip))
	#p3 = Process(target=Reporting,args=(database_q,))
	#p5 = Process(target=ReportingRefresh)
	#pcronjob = Process(target=CronJob)
	if(cloudconfig.read_manual_json_flag==True):
		p_manualjsonread = Process(target=ManualJsonRead, args=(orch_q,))
		p_manualjsonread.start()
	if(cloudconfig.ondemand_flag):
		pondemand = Process(target=Ondemand)
		pondemand.start()
	#p4 = Process(target=RequestData, args=(orch_q, configDict['company'])) #make sure to write company name here # 'ulvaprod

	#pcronjob.start()
	#p5.start()
	p.start()
	p2.start()      
	#p4.start()
		
    



