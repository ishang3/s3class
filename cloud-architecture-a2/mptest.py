from multiprocessing import Process, Queue
import random
from aiokafka import AIOKafkaConsumer
import asyncio
import time


def f(q):
    q.put(11)

def f3(q):
    while True:
        time.sleep(1)
        for item in iter(q.get, None):
            print('bad')
            print(item)



class KafkaSubscribe():
    def __init__(self,q):
        self.loop = asyncio.get_event_loop()
        self.q = q
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
                self.q.put(msg.value)
                #self.ret(msg.value)
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()




if __name__ == '__main__':
    q = Queue()
    p = Process(target=f, args=(q,))
    p2 = Process(target=KafkaSubscribe,args=(q,))
    p3 = Process(target=f3,args=(q,))
    p.start()
    p3.start()
    p2.start()  
    


 
    