import os

def sendtokafka(message):
	from confluent_kafka import Producer
	import json

	brokerurl = '13.59.52.241:9092' #'13.59.52.241:9092' #'3.22.100.122:9092'

	p = Producer({'bootstrap.servers': brokerurl})

	def delivery_report(err, msg):
		""" Called once for each message produced to indicate delivery result.
			Triggered by poll() or flush(). """
		if err is not None:
			print('Message delivery failed: {}'.format(err))
		else:
			print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

	# Trigger any available delivery report callbacks from previous produce() calls
	msg = json.dumps(message)#.encode('utf-8')
	# f = open('./new-dev/box-delivered.json', "r")
	# msg = json.loads(f.read())
	# msg = json.dumps(msg)
	# print('ISHAN ISHAN ISHAN ')
	# print(msg)
	# print('ISHAN ISHAN ISHAN ')
	p.poll(0)

	# Asynchronously produce a message, the delivery report callback
	# will be triggered from poll() above, or flush() below, when the message has
	# been successfully delivered or failed permanently.
	print(msg)
	p.produce('return-analysis', msg, callback=delivery_report)

	# Wait for any outstanding messages to be delivered and delivery report
	# callbacks to be triggered.
	p.flush()
	print('RETURN JSON SENT BACK!!!!!!!!')


# sendtokafka('ishan!')