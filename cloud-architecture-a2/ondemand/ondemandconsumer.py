from confluent_kafka import Consumer, KafkaException
import sys
import json
import logging
from pprint import pformat
import time
import json
from payload.payload import Payload
from ondemand.database.Postgres import Database
import os
import queue
import threading
from confluent_kafka import Consumer, KafkaException
from aiokafka import AIOKafkaConsumer
import asyncio
from multiprocessing import Process, Queue
import random
import cloudconfig



class Ondemand():
    def __init__(self,):
        print("Initializing ondemand class!")
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.consume())
		

    async def consume(self,):
        if(cloudconfig.kafka_server=='prod'):
            ip = cloudconfig.kafka_prod_ip
        elif(cloudconfig.kafka_server=='dev'):
            ip = cloudconfig.kafka_dev_ip
        else:
            print("unknown kafka server. please specify prod or dev")

        consumer = AIOKafkaConsumer(
			'ondemand_request',
    		loop=self.loop, bootstrap_servers=ip, #dev 3.22.100.122:9092 #prod 13.59.52.241:9092
			group_id="sr1")
		# Get cluster layout and join group `my-group`
        await consumer.start()
        print("ondemand consumer started")
        try:
			# Consume messages
            async for msg in consumer:
                msg = json.loads(msg.value)

                print(msg)
                print('********&&&&')

                payload = Payload(msg,orch=False)
                # #create database instance

                database = Database(payload.dict['cameraid'],
                            payload.dict['logId'],
                            payload.dict['human-readable-regex'])

                database.generate_report(payload.commands,
                                 payload.dict['table_name'])
    
        finally:
			#Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()

# test = Ondemand()