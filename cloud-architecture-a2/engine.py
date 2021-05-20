from orchestrator import Orchestrator
from regionorchestrator import RegionOrchestrator
from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
from pprint import pformat
import time
from s3 import GetObject
from payload.payload import Payload
from utils import test
from utils import convert_upload
from pytorch.utilspytorch import utilspytorch
from sqlops.Postgres import Database
from sqlops.pub import sendtokafka
import json


if __name__ == '__main__':

    broker = '3.22.100.122:9092'
    # group = argv[1]
    topics = ['start-analysis']
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker, 'group.id': 2,
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

    # Read messages from Kafka, print to stdout
    try:
        while True:

            msg = c.poll(timeout=1.0)

            if msg is None:
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

                print(msg)
                continue
                #exit()
                # insert something in return json to check if it is, then just continue
                checkifreturn = json.loads(msg)
                returnjson = False


                payload = Payload(msg)
                object = 'forklifts'



                #get = GetObject(payload.dict['videobucket'], payload.dict['videokey'])  # this will download the respective video into the folder
                orchestrate = Orchestrator(payload.dict['anns'], payload.dict['lines'], payload.dict['regex'],
                                           payload.commands, payload.dict['videokey'])
                region_orchestrate = RegionOrchestrator(payload.dict['anns'],payload.dict['lines'], payload.dict['region-regex'], payload.commands, payload.dict['videokey'])


                #if object to be identified is a forklift then run a different orchestrator
                if object in payload._objects:
                    # then doo the necessary things
                    with open('sqlops/forklift-return-final.json') as f:
                        data = json.load(f)
                        sendtokafka(data)


                else:
                    for ret in test(payload.dict['anns']):
                        orchestrate._do(ret)
                        region_orchestrate._do(ret)

                    #this will necessary upload the video to s3
                    convert_upload()

                    #this will create the return json with the reports and send it too
                    print('Now Generating reports')
                    database = Database(payload.dict['human-readable-regex'])
                    database.generate_report(payload.commands)
                    print('Reports are saved.')


                    print('DONE!!!!!!!')





    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()