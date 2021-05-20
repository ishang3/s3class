import os
import sys
import cv2
import torch
import numpy as np
import torch.utils.data as data
import xml.etree.ElementTree as ET

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from config import opt
from lib.augmentations import preproc_for_test, preproc_for_train

VOC_LABELS = (
        "box",
        "person",
        "stack_of_boxes",
        "forklift",
        "forklift_arm",
        "amr",
        "loading_truck",
        "bins",
        "stack_of_bins",
        "rack",
        "cart",
        "pallet",
        "stack_of_pallets",
        "don't_care",
    )

# VOC_LABELS = (
#     'forklift',
#     'forklift_arm',
#     'pallet',
#     'loaded_pallet',
#     'dont_care',
#     'person',
#     'amr',
#     'cardboard'
# )


class VOCDetection(data.Dataset):


    def __init__(self, opt, image_sets=[['2012', 'train']], is_train=True):


        self.root = opt.VOC_TRAIN_ROOT
        self.image_sets = image_sets
        self.is_train = is_train
        self.opt = opt    
    
        self.ids = []
        for (year, name) in self.image_sets:
            root_path = os.path.join(self.root, 'VOC' + year)
            ano_file = os.path.join(root_path, 'ImageSets', 'Main', name + '.txt')
    
            with open(ano_file, 'r') as f:
                for line in f.readlines():
                    line = line.strip()
                    ano_path = os.path.join(root_path, 'Annotations', line + '.xml')
                    img_path = os.path.join(root_path, 'JPEGImages', line + '.jpg')
                    img_path_png = os.path.join(root_path, 'JPEGImages', line + '.png')
                    img_path_jpeg = os.path.join(root_path, 'JPEGImages', line + '.jpeg')
                    self.ids.append((img_path, img_path_png, img_path_jpeg, ano_path))


    
    def __getitem__(self, index):
        #img_path, ano_path = self.ids[index]
        img_path, img_path_png, img_path_jpeg, ano_path = self.ids[index]
        image = cv2.imread(img_path, cv2.IMREAD_COLOR)
        if image is None:
            image = cv2.imread(img_path_png, cv2.IMREAD_COLOR)
            if image is None:
                image = cv2.imread(img_path_jpeg, cv2.IMREAD_COLOR)

        boxes, labels = self.get_annotations(ano_path)
        
        if self.is_train:
            image, boxes, labels = preproc_for_train(image, boxes, labels, opt.min_size, opt.mean)
            image = torch.from_numpy(image)
           
        
        
        target = np.concatenate([boxes, labels.reshape(-1,1)], axis=1)
        
        return image, target



    def get_annotations(self, path):
        
        tree = ET.parse(path)

        boxes = []
        labels = []
        
        for child in tree.getroot():
            if child.tag != 'object':
                continue

            bndbox = child.find('bndbox')
            box =[
                float(bndbox.find(t).text) - 1
                for t in ['xmin', 'ymin', 'xmax', 'ymax']
            ]


            label = VOC_LABELS.index(child.find('name').text.lower().strip()) 
            
            boxes.append(box)
            labels.append(label)
        
        return np.array(boxes), np.array(labels)
            

        

    def __len__(self):
        return len(self.ids)
        
