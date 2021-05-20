from aiokafka import AIOKafkaConsumer
import asyncio
from multiprocessing import Process, Queue
import random


class ReportingRefresh():
	def __init__(self,):
		self.loop = asyncio.get_event_loop()
		self.loop.run_until_complete(self.consume())

	async def consume(self,):
		consumer = AIOKafkaConsumer(
			'restart_report',
    		loop=self.loop, bootstrap_servers='3.22.100.122:9092',
			group_id="1")
		# Get cluster layout and join group `my-group`
		await consumer.start()
		try:
			# Consume messages
			async for msg in consumer:
				print(msg.value,'**************')
   
			
		finally:
			#Will leave consumer group; perform autocommit if enabled.
			await consumer.stop()