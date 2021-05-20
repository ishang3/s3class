from centroid.centroidtracker import CentroidTracker
import numpy as np
import argparse
import imutils
import time
import cv2
import json
import random
import os
from os.path import isfile, join
import natsort
import shutil
import boto3


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def convert_upload(cameraid):

	pathIn = './output/'
	pathOut = 'output.mp4'
	fps = 24
	frame_array = []
	files = [f for f in os.listdir(pathIn) if isfile(join(pathIn, f))]

	s3key = str(cameraid) + 'output.mp4'

	print('Rendering video with annotations')
	files = natsort.natsorted(files)
	for i in range(len(files)):
		filename = pathIn + files[i]
		# reading each files
		img = cv2.imread(filename)
		height, width, layers = img.shape
		size = (width, height)

		# inserting the "frames into an image array
		frame_array.append(img)
	out = cv2.VideoWriter(pathOut, cv2.VideoWriter_fourcc(*'X264'), fps, size)
	for i in range(len(frame_array)):
		# writing to a image array
		out.write(frame_array[i])
	out.release()

	#### push to s3 bucket
	print('Uploading File to S3')
	client = boto3.client('s3')
	client.upload_file('output.mp4', 'completedvideo', s3key)
	os.remove('output.mp4')

def test(bboxes):

	newpath = './output'
	# delete any temp directory and its contents
	if os.path.exists(newpath):
		shutil.rmtree('./output')
	# if directory is not yet created, then create one
	if not os.path.exists(newpath):
		os.makedirs(newpath)


	ct = CentroidTracker()
    # iotcore = PublishTopic()

	(H, W) = (None, None)

	# THIS WILL BE A ML LEARNING APPROACH

	CLASSES = ["background", "aeroplane", "bicycle", "bird", "boat",
               "bottle", "bus", "car", "cat", "chair", "cow", "diningtable",
               "dog", "horse", "motorbike", "person", "pottedplant", "sheep",
               "sofa", "train", "tvmonitor"]

	# initialize OpenCV's special multi-object tracker
	# trackers = cv2.MultiTracker_create()

	# load our serialized model from disk
	print("[INFO] loading model...")
	net = cv2.dnn.readNetFromCaffe('models/mobilenet_ssd/MobileNetSSD_deploy.prototxt',
                                   'models/mobilenet_ssd/MobileNetSSD_deploy.caffemodel')

    # initialize the video stream and allow the camera sensor to warmup
    # print("[INFO] starting video stream...")
    # cap = cv2.VideoCapture(0)

	cap = cv2.VideoCapture('temp/video.mp4')
	#cap = cv2.VideoCapture('videos/people.mp4')
	time.sleep(2.0)

	total_points = {}
	color_id = {}
	total = []
	count = 0
	detections_in_frame = []
	detection = False
    # loop over the frames from the video stream
	while True:
		# read the next frame from the video stream and resize it
		# frame = vs.read()
		ret, frame = cap.read()
		if not ret:
			#do a bit of cleanup
			cv2.destroyAllWindows()
			break
		#frame = imutils.resize(frame, width=500)
		frame = cv2.resize(frame, (576, 320))
		# if the frame dimensions are None, grab them
		if W is None or H is None:
			(H, W) = frame.shape[:2]
		# blob = cv2.dnn.blobFromImage(frame, 1.0, (W, H),(104.0, 177.0, 123.0))
		blob = cv2.dnn.blobFromImage(frame, 0.007843, (W, H), 127.5)
		net.setInput(blob)
		detections = net.forward()
		rects = {}
		rects_counter = 0
    	# loop over the detections
		for i in range(0, detections.shape[2]):
			if detections[0, 0, i, 2] > 0.5:
				# $#$#$#$34 Remove when not using person detectio model
				# extract the index of the class label from the
				# detections list
				idx = int(detections[0, 0, i, 1])
				# if the class label is not a person, ignore it
				if CLASSES[idx] != "person":
					continue
				# compute the (x, y)-coordinates of the bounding box for
				# the object, then update the bounding box rectangles list
				box = detections[0, 0, i, 3:7] * np.array([W, H, W, H])
				detections_in_frame.append(box)
				#rects.append(box.astype("int"))
				rects[rects_counter] = (box.astype("int"), 'person')
				rects_counter += 1

				(startX, startY, endX, endY) = box.astype("int")

		objects, last_location, mappings = ct.update(rects)
		# loop over the tracked objects
		total = []
		for (objectID, centroid) in objects.items():
			# draw both the ID of the object and the centroid of the
			# object on the output frame
			text = "ID {}".format(objectID)
			cv2.putText(frame, text, (centroid[0] - 10, centroid[1] - 10),
				cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
			cv2.circle(frame, (centroid[0], centroid[1]), 4, (0, 255, 0), -1)
			total.append((objectID,centroid))
			#yield objectID,centroid
		yield total, mappings


		for (objectID, centroid_list) in last_location.items():
			if len(centroid_list) == 2:
				p1 = (int(centroid_list[0][0]), int(centroid_list[0][1]))
				p2 = (int(centroid_list[1][0]), int(centroid_list[1][1]))

				# Method # 1

				total.append((p1, p2))

				# Method # 2
				if objectID not in total_points:
					total_points[objectID] = [(p1, p2)]
					color_id[objectID] = (random.randint(100, 250), random.randint(
						100, 250), random.randint(150, 255))
				else:
					total_points[objectID].append((p1, p2))
			    # thickness = int(np.sqrt(32 / float(i + 1)) * 50)
			    # (p1,p2)
			    # method # 1
				# for x in total:
				# 	cv2.line(frame, x[0], x[1], (255, 0, 0), 10)

			    # method # 2
			    # print(total_points.keys(),total_points.values())
				for key, value in total_points.items():
					for point in value:
						cv2.line(frame, point[0], point[1], color_id[key], 10)
						#cv2.line(frame, point[0], point[1], (0, 0, 255), 10)
				#yield total_points


		total = []
		detections_in_frame = []

		# show the output frame
		# this is for people process vide
		# image = cv2.rectangle(frame, (239, 1),(449, 133), (255,0,0), 2)
		# image = cv2.rectangle(frame, (72,65), (216,129), (0, 255, 0), 2)
		# image = cv2.rectangle(frame, (197, 135), (422, 305), (0, 0, 255), 2)

		#this is for the station video
		# image = cv2.rectangle(frame, (142,102), (316,217), (0, 255, 0), 2)
		# image = cv2.rectangle(frame, (346,11), (508,101), (0, 0, 255), 2)

		for region in bboxes:
			values = bboxes[region]
			cv2.rectangle(frame, (values[0], values[1]), (values[2], values[3]), (0, 0, 255), 2)


		#this is for station monitoring video
		cv2.imshow("Frame", frame)

		cv2.imwrite("output/image" + str(count) + ".jpg", frame)
		count+=1
		key = cv2.waitKey(1) & 0xFF

		# if the `q` key was pressed, break from the loop
		if key == ord("q"):
			break
