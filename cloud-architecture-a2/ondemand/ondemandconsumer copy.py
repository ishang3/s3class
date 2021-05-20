from confluent_kafka import Consumer, KafkaException
import sys
import json
import logging
from pprint import pformat
import time
import json
from payload.payload import Payload
from database.Postgres import Database
import os
import queue
import threading
from confluent_kafka import Consumer, KafkaException
from aiokafka import AIOKafkaConsumer
import asyncio
from multiprocessing import Process, Queue
import random



class KafkaSubscribe():
	def __init__(self,q,db_q):
		self.deltable = Clear() 
		self.drop = Drop()
		self.loop = asyncio.get_event_loop()
		self.q = q
		self.db_q = db_q
		self.loop.run_until_complete(self.consume())
		

	async def consume(self,):
		consumer = AIOKafkaConsumer(
			'start-analysis',
    		loop=self.loop, bootstrap_servers=os.environ.get('brokerurl'), #dev 3.22.100.122:9092 #prod 13.59.52.241:9092
			group_id="1")
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
					# with open("sample.json", "w") as outfile:
					# 	json.dump(data, outfile) 

					self.q.put(msg.value)
					self.db_q.put(msg.value)
			
		finally:
			#Will leave consumer group; perform autocommit if enabled.
			await consumer.stop()


if __name__ == '__main__':

    #brokerurl = os.environ.get('broker')

    broker = '13.59.52.241:9092'  #brokerurl #dev 3.22.100.122:9092 #prod 13.59.52.241:9092
    # group = argv[1]
    topics = ['ondemand_request']
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker, 'group.id': 1,
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest',
            "max.poll.interval.ms": "86400000"}

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages

    c = Consumer(conf)

    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to  stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                # with open('/Users/ishan/Documents/index9/new-jsons/new-jsons/pose/POSE-RAW.json') as f:
                #     data = json.load(f)
                #     sendtokafka(data)
                continue
            if msg.error():
                continue
                #raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                msg = msg.value().decode('utf-8')
                msg = json.loads(msg)
            

                payload = Payload(msg,orch=False)
                print(payload.commands)

                

                #create database instance

                database = Database(payload.dict['cameraid'],
                            payload.dict['logId'],
                            payload.dict['human-readable-regex'])

                database.generate_report(payload.commands,
                                 payload.dict['table_name'])
                
                

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()