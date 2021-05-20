from centroid.centroidtracker import CentroidTracker
import numpy as np
import argparse
import imutils
import cv2
import json
import random
import time
from voc_dataset import VOCDetection
from confignew import opt
import numpy as np
from lib.vgg_model import VGG_SSD
from lib.res_model import RES18_SSD, RES101_SSD
from libnew.network_resnet import RESNET_SSD
from lib.resnet import *
import torch
import torch.nn.functional as F
import os
from lib.utils import detection_collate
from lib.multibox_encoder import MultiBoxEncoder
from lib.ssd_loss import MultiBoxLoss
import cv2
from lib.utils import nms
from lib.augmentations import preproc_for_test
import matplotlib.pyplot as plt
from lib.utils import detect
#from voc_dataset import VOC_LABELS
from voc_train_data_loader import VOC_LABELS
from os.path import isfile, join
import natsort
import shutil
import boto3

#loading the model
#model = VGG_SSD(opt.num_classes, opt.anchor_num)
# model = RES18_SSD(opt.num_classes, opt.anchor_num,pretrain=False)
# model = RES101_SSD(opt.num_classes, opt.anchor_num,pretrain=False)

model = RESNET_SSD(opt.backbone_network_name, opt.num_classes, opt.anchor_num)

state_dict = torch.load('./weight/locloss_0.470_clsloss_1.330_total_loss_322.572.pth', map_location='cpu')
#state_dict = torch.load('./weight/11_2_model.pth', map_location='cpu')
from collections import OrderedDict

new_state_dict = OrderedDict()
for k, v in state_dict.items():
	name = k[7:] # remove `module.`
	new_state_dict[name] = v

#model.load_state_dict(new_state_dict)
model.load_state_dict(state_dict)
multibox_encoder = MultiBoxEncoder(opt)

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)

def utilspytorch(lines,classes=None):
	#classes = ['forklift','person','pallet','box']
	newpath = './output'
	# delete any temp directory and its contents
	if os.path.exists(newpath):
		shutil.rmtree('./output')
	# if directory is not yet created, then create one
	if not os.path.exists(newpath):
		os.makedirs(newpath)

	ct = CentroidTracker()
	cap = cv2.VideoCapture('temp/video.mp4')
	time.sleep(2.0)
	count = 0
	total_points = {}
	color_id = {}

	while True:
		# read the next frame from the video stream and resize it
		# frame = vs.read()
		ret, src = cap.read()
		if not ret:
			#do a bit of cleanup
			break
			cv2.destroyAllWindows()

		#src = cv2.imread(image,cv2.IMREAD_COLOR)
		image = preproc_for_test(src, opt.min_size, opt.mean)
		image = torch.from_numpy(image)

		with torch.no_grad():
			loc, conf = model(image.unsqueeze(0))
		loc = loc[0]
		conf = conf[0]
		conf = F.softmax(conf, dim=1)
		conf = conf.numpy()
		loc = loc.numpy()
		decode_loc = multibox_encoder.decode(loc)
		gt_boxes, gt_confs, gt_labels = detect(decode_loc, conf, nms_threshold=0.45, gt_threshold=0.1)

		h, w = 320, 576
		src = cv2.resize(src, (576, 320))
		gt_boxes[:, 0] = gt_boxes[:, 0] * w
		gt_boxes[:, 1] = gt_boxes[:, 1] * h
		gt_boxes[:, 2] = gt_boxes[:, 2] * w
		gt_boxes[:, 3] = gt_boxes[:, 3] * h
		gt_boxes = gt_boxes.astype(int)

		rects = {}
		rects_counter = 0
		for box, label, score in zip(gt_boxes, gt_labels, gt_confs):
			if VOC_LABELS[label] in classes:
				object_type = VOC_LABELS[label]
				if score > 0.3:
					image = cv2.rectangle(src, (box[0], box[1]), (box[2], box[3]), (0, 0, 255), 2)
					rect = np.array([box[0], box[1], box[2], box[3]])
					rects[rects_counter] = (rect,object_type)
					rects_counter += 1
			# loop over the tracked objects

		objects, last_location, mappings = ct.update(rects)
		#print(mappings, '*#*$*#$*#$*$#*')
		total = []
		for (objectID, centroid) in objects.items():
			# draw both the ID of the object and the centroid of the
			# object on the output frame
			text = "ID {}".format(objectID)
			cv2.putText(src, text, (centroid[0] - 10, centroid[1] - 10),
						cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
			cv2.circle(src, (centroid[0], centroid[1]), 4, (0, 255, 0), -1)
			total.append((objectID, centroid))
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
						cv2.line(src, point[0], point[1], color_id[key], 10)
				# cv2.line(frame, point[0], point[1], (0, 0, 255), 10)
		# yield total_points

		total = []
		detections_in_frame = []

		# for region in bboxes:
		# 	values = bboxes[region]
		# 	cv2.rectangle(src, (values[0], values[1]), (values[2], values[3]), (0, 0, 255), 2)
		for line in lines:
			vals = lines[line]['coords']
			cv2.line(src, (vals[0], vals[1]), (vals[2], vals[3]), (255, 0, 0), 5)

		#this is for station monitoring video
		cv2.imshow("Frame", src)

		cv2.imwrite("output/image" + str(count) + ".jpg", src)
		count+=1
		key = cv2.waitKey(1) & 0xFF

		# if the `q` key was pressed, break from the loop
		if key == ord("q"):
			break


# for ret in utilspytorch():
#     continue