from aiokafka import AIOKafkaConsumer
import asyncio
import time



class KafkaSubscribe():
    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.vals = []
        self.loop.run_until_complete(self.consume())

    def ret(self,message):
        print(message)


    async def consume(self,):
        consumer = AIOKafkaConsumer(
            'start-analysis',
            loop=self.loop, bootstrap_servers='3.22.100.122:9092',
            group_id="1")
        # Get cluster layout and join group `my-group`
        await consumer.start()
        try:
            # Consume messages
            async for msg in consumer:
                # print("consumed: ", msg.topic, msg.partition, msg.offset,
                #     msg.key, msg.value, msg.timestamp)  
                self.vals.append(msg.value)
                print(self.vals)
                #self.ret(msg.value)
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()


    

subscribe = KafkaSubscribe()



# loop = asyncio.get_event_loop()

# async def consume():
#     consumer = AIOKafkaConsumer(
#         'start-analysis',
#         loop=loop, bootstrap_servers='3.22.100.122:9092',
#         group_id="1")
#     # Get cluster layout and join group `my-group`
#     await consumer.start()
#     try:
#         # Consume messages
#         async for msg in consumer:
#             print("consumed: ", msg.topic, msg.partition, msg.offset,
#                   msg.key, msg.value, msg.timestamp)
#     finally:
#         # Will leave consumer group; perform autocommit if enabled.
#         await consumer.stop()


# async def do_io():
#     while True:
#         await asyncio.sleep(1)
#         print('ishan')

# loop.create_task(consume())
# loop.run_until_complete(do_io())


