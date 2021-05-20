# import the necessary packages
from scipy.spatial import distance as dist
from collections import OrderedDict
import numpy as np
import cloudconfig

class CentroidTracker():
	def __init__(self,pose=False,maxDisappeared=cloudconfig.tracker_hysteresis): #50 for people process videos
		# initialize the next unique object ID along with two ordered
		# dictionaries used to keep track of mapping a given object
		# ID to its centroid and number of consecutive frames it has
		# been marked as "disappeared", respectively
		self.nextObjectID = 0
		self.objects = OrderedDict()
		self.objects_last_location = OrderedDict() #I can get the last location of the box
		self.disappeared = OrderedDict()
		self.object_type_mappings = {}
		self.centroid_to_type_mappings = {}
		self.pose_flag = pose
		# store the number of maximum consecutive frames a given
		# object is allowed to be marked as "disappeared" until we
		# need to deregister the object from tracking
		self.maxDisappeared = maxDisappeared


		#below is for bbox visualization
		self.centroid_bbox_visulization = {}
		self.object_bbox_mappings = {}

	def register(self,centroid,type):
		# when registering an object we use the next available object
		# ID to store the centroid
		self.objects[self.nextObjectID] = centroid
		#associate the objectid with its type
		self.object_type_mappings[self.nextObjectID] = type
		self.disappeared[self.nextObjectID] = 0
		self.objects_last_location[self.nextObjectID] = [centroid]

		#visualization purposes
		self.object_bbox_mappings[self.nextObjectID] = self.centroid_bbox_visulization[tuple(centroid)]

		self.nextObjectID += 1

		

	def deregister(self, objectID):
		# to deregister an object ID we delete the object ID from
		# both of our respective dictionaries
 
		del self.objects[objectID]
		del self.disappeared[objectID]
		del self.objects_last_location[objectID]
		del self.object_type_mappings[objectID]
		#visualiztion purposes
		del self.object_bbox_mappings[objectID]
		

	def update(self, rects):
		# check to see if the list of input bounding box rectangles
		# is empty
		if len(rects) == 0:
			# loop over any existing tracked objects and mark them
			# as disappeared
			for objectID in list(self.disappeared.keys()):
				self.disappeared[objectID] += 1

				# if we have reached a maximum number of consecutive
				# frames where a given object has been marked as
				# missing, deregister it
				if self.disappeared[objectID] > self.maxDisappeared:
					self.deregister(objectID)

			# return early as there are no centroids or tracking info
			# to update

			return self.objects,self.objects_last_location,self.object_type_mappings,self.object_bbox_mappings

		# initialize an array of input centroids for the current frame
		inputCentroids = np.zeros((len(rects), 2), dtype="int")
		inputMappings = {}
		# loop over the bounding box rectangles
		i = 0
		self.centroid_to_type_mappings = {}
		#visualiztion purpose; empty out here
		self.centroid_bbox_visulization = {}
		for key in rects:
			value = rects[key]
			bbox,bbox_type = value
			
			startX, startY, endX, endY = bbox
			# use the bounding box coordinates to derive the centroid
			cX = int((startX + endX) / 2.0)
			cY = int((startY + endY) / 2.0)
			inputCentroids[i] = (cX, cY)
			inputMappings[str(i)] = bbox_type
			self.centroid_to_type_mappings[tuple([cX,cY])] = bbox_type
			self.centroid_bbox_visulization[tuple([cX,cY])] = bbox
			i += 1

		# # loop over the bounding box rectangles
		# for (i, (startX, startY, endX, endY)) in enumerate(rects):
		# 	# use the bounding box coordinates to derive the centroid
		# 	cX = int((startX + endX) / 2.0)
		# 	cY = int((startY + endY) / 2.0)
		# 	inputCentroids[i] = (cX, cY)

		# if we are currently not tracking any objects take the input
		# centroids and regtaister each of them
		if len(self.objects) == 0:
			for i in range(0, len(inputCentroids)):
				self.register(inputCentroids[i],inputMappings[str(i)])

		# otherwise, are are currently tracking objects so we need to
		# try to match the input centroids to existing object
		# centroids
		else:
			# grab the set of object IDs and corresponding centroids
			objectIDs = list(self.objects.keys())
			objectCentroids = list(self.objects.values())

			# compute the distance between each pair of object
			# centroids and input centroids, respectively -- our
			# goal will be to match an input centroid to an existing
			# object centroid
			D = dist.cdist(np.array(objectCentroids), inputCentroids)

			# in order to perform this matching we must (1) find the
			# smallest value in each row and then (2) sort the row
			# indexes based on their minimum values so that the row
			# with the smallest value as at the *front* of the index
			# list
			rows = D.min(axis=1).argsort()

			# next, we perform a similar process on the columns by
			# finding the smallest value in each column and then
			# sorting using the previously computed row index list
			cols = D.argmin(axis=1)[rows]

			# in order to determine if we need to update, register,
			# or deregister an object we need to keep track of which
			# of the rows and column indexes we have already examined
			usedRows = set()
			usedCols = set()

			# loop over the combination of the (row, column) index
			# tuples
			for (row, col) in zip(rows, cols):
				# if we have already examined either the row or
				# column value before, ignore it
				# val
				if row in usedRows or col in usedCols:
					continue

				# otherwise, grab the object ID for the current row,
				# set its new centroid, and reset the disappeared
				# counter
				objectID = objectIDs[row]
				self.objects[objectID] = inputCentroids[col]
				self.disappeared[objectID] = 0

				#if Flag is set to True then do the updates accordingly
				if self.pose_flag:
					self.object_type_mappings[objectID] = self.centroid_to_type_mappings[tuple(inputCentroids[col])]
					self.object_bbox_mappings[objectID] = self.centroid_bbox_visulization[tuple(inputCentroids[col])]

				if len(self.objects_last_location[objectID]) == 2:
					self.objects_last_location[objectID].pop(0)
					self.objects_last_location[objectID].append(inputCentroids[col])
				else:
					self.objects_last_location[objectID].append(inputCentroids[col])

				#need to update the last locations here of the centroids

				# indicate that we have examined each of the row and
				# column indexes, respectively
				usedRows.add(row)
				usedCols.add(col)

			# compute both the row and column index we have NOT yet
			# examined
			unusedRows = set(range(0, D.shape[0])).difference(usedRows)
			unusedCols = set(range(0, D.shape[1])).difference(usedCols)

			# in the event that the number of object centroids is
			# equal or greater than the number of input centroids
			# we need to check and see if some of these objects have
			# potentially disappeared
			if D.shape[0] >= D.shape[1]:
				# loop over the unused row indexes
				for row in unusedRows:
					# grab the object ID for the corresponding row
					# index and increment the disappeared counter
					objectID = objectIDs[row]
					self.disappeared[objectID] += 1

					# check to see if the number of consecutive
					# frames the object has been marked "disappeared"
					# for warrants deregistering the object
					if self.disappeared[objectID] > self.maxDisappeared:
						self.deregister(objectID)

			# otherwise, if the number of input centroids is greater
			# than the number of existing object centroids we need to
			# register each new input centroid as a trackable object
			else:
				for col in unusedCols:
					self.register(inputCentroids[col],inputMappings[str(col)])

		#run Hao's Code Here


		# return the set of trackable objects
		return self.objects,self.objects_last_location,self.object_type_mappings,self.object_bbox_mappings
