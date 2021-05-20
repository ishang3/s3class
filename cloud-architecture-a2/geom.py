"""
This base class serves as all things related to regions and lines
"""
from shapely.geometry import Polygon
import json
from shapely.geometry import Point
from shapely.geometry import box
import shapely.geometry as sg
import shapely.ops as so
import matplotlib.pyplot as plt
from shapely.geometry import LineString

class Geometry:

    def __init__(self,regions,lines):
        self.lines = {}
        self.polygons = {}
        self._initialize_region(regions)
        self._initialize_line(lines)

    def check_side(self,point):
        # inputs: 1 point (x,y)

        ret = []
        x,y = point.x,point.y
        for line in self.lines:
            slope = self.lines[line]['slope']
            x1, y1, x2, y2 = self.lines[line]['coords']
            side = y < slope * (x - x1) + y1 
            if side:
                ret.append(self.lines[line]['less'])
            if not side:
                ret.append(self.lines[line]['greater'])
        # debug print
        #print("print, ret")
        #print(point, ret)
        
        return ret


    def _initialize_line(self,lines):
        """
        This will read the lines and the associated points on both sides
        """

        if len(lines) == 0:
            return

        for line in lines:
            values = lines[line]['coords']
            line_coords = [(values[0], values[1]), (values[2], values[3])] #xmin,ymin,xmax,ymax
        
            self.lines[line] = { 'coords' : (values[0], values[1], values[2], values[3]) }

            # calculate slope
            x1, y1, x2, y2 = values
            if x2 - x1 != 0 and y2 - y1 != 0:
                slope = (y2 - y1) / (x2 - x1)

            # this means the slope of the line is vertical or horizontal
            # then we can check if horizontal and vertical line
            else:
                self.lines[line]['slope'] = 0
                vertical = x2 - x1 == 0
                horizontal = y2 - y1 == 0

                if vertical:
                    x,y = lines[line]['S1']
                    if x < x1:
                        self.lines[line]['less'] = line + 'S1'
                        self.lines[line]['greater'] = line +'S2'
                    else:
                        self.lines[line]['less'] = line + 'S2'
                        self.lines[line]['greater'] = line + 'S1'
                    return

                if horizontal:
                    x, y = lines[line]['S1']
                    if y > y1:
                        self.lines[line]['greater'] = line + 'S1'
                        self.lines[line]['less'] = line + 'S2'
                    else:
                        self.lines[line]['greater'] = line + 'S2'
                        self.lines[line]['less'] = line + 'S1'
                    return

                return

            #this logic will follow if slop of the line is no 0
            self.lines[line]['slope'] = slope
            #print("slope", slope)

            # checking which side S1 and S2 belong to
            s1_x, s1_y = lines[line]['S1']
            s2_x, s2_y = lines[line]['S2']
            #print("S1", s1_x, s1_y)
            #print("S2",s2_x,s2_y)
            side = s1_y < slope * (s1_x - x1) + y1
            if side:
                #print("S1 is on the less side")
                self.lines[line]['less'] = line + 'S1'
                self.lines[line]['greater'] = line + 'S2'
            else:
                #print("S2 is on the less side")
                self.lines[line]['less'] = line + 'S2'
                self.lines[line]['greater'] = line + 'S1'

            '''
            #doing side 1
            x,y = lines[line]['S1']
            print("S1", x, y)
            side = y < slope * (x - x1) + y1
            if side:
                print("S1 is on the less side, block1")
                self.lines[line]['less'] = line + 'S1'
            else:
                print("S2 is on the greater side, block1")
                self.lines[line]['greater'] = line + 'S2'


            # doing side 2
            x,y = lines[line]['S2']
            print("S2",x,y)
            side = y < slope * (x - x1) + y1
            if side:
                print("S1 is on the less side, block2")
                self.lines[line]['less'] = line + 'S1'
            else:
                print("S2 is on the greater side, block2")
                self.lines[line]['greater'] = line + 'S2'
            '''
            # debug prints
            #print('printing self.lines')
            #print(self.lines)



    def _initialize_region(self,regions):
        """
        This will read the regions from the input file
        and convert them into a Polygon object

         Parameters
        __________
        regions: file location
        """
        #open text with predefined regions


        # for the people process video
        # self.polygons['R0'] = box(239, 1, 449, 133)
        # self.polygons['R1']  = box(72,65,216,129)
        # self.polygons['R2'] = box(197, 135, 422, 305)

        for region in regions:
            values = regions[region]
            self.polygons[region] = box(values[0],values[1],values[2],values[3])

        # self.polygons['R0'] =  box(257,92,345,292)
        # self.polygons['R1'] =  box(495,94,566s,265)

    @staticmethod
    def create_box(xmin,ymin,xmax,ymax):
        """
        This method will take in the input
        """
        return box(xmin,ymin,xmax,ymax)

    @staticmethod
    def create_centroid(xmin, ymin, xmax, ymax):
        """
		This method will take the bounding box
		and return a centroid
		"""
        return box(xmin, ymin, xmax, ymax).centroid

    @staticmethod
    def create_point(centroid):
        """
        This method will create a point shapely object
        """
        return Point(centroid[0], centroid[1])


    def add(self,coords,typeOfRegion,title):
        """
        After initialization if a user wants to add a new region

        coords: coordinate points respective to object
                line: [(0, 0), (1, 1)]
                box: (xmin,ymin,xmax,ymax)
        typeOfRegion: box or Line
        title: name of region
        """
        if typeOfRegion ==  'line':
            self.lines[title] = LineString(coords)
        if typeOfRegion == 'box':
            self.polygons[title] = box(coords)

    def obj_state(self, point):
        """
        Returns implict states wrt to current regions

        If True, returns True and the respective region

        Parameters
        __________
        point: tuple
         the centroid of the bounding box

        Returns:
         Dictionary: exhaustive set of what regions are captured

        {'BOX Region Identifier': 1 or 0,
        'Line': S1 or S2 or 1
        }
        """

        boxStates = {}
        # this for loop iterates through all boxes
        for key in self.polygons:
            polygon = self.polygons[key]
            if polygon.contains(point):
                boxStates[key] = 1
            else:
                boxStates[key] = 0

        lineStates = {}
        if len(self.lines) != 0:
            list_states = self.check_side(point)
            for state in list_states:
                lineStates[str(state)] = 1

        return self.process_usecase_state(boxStates,lineStates)

    def process_usecase_state(self,boxStates,lineStates):
        """
        
        """
        ret = []
        for state in boxStates:
            if boxStates[state] == True: # if any of the keys contain 1 as their value
                ret.append(state) # this should only have 1 value appended
                # break
        for state in lineStates:
            if lineStates[state] == True:
                ret.append(state)
        #print(ret,'ISHAN')
        return ret

'''

https://math.stackexchange.com/questions/274712/calculate-on-
which-side-of-a-straight-line-is-a-given-point-located

>>> Point(0, 0).geom_type
'Point'

>>> box.name
'R1'


To determine which side of the line from 𝐴=(𝑥1,𝑦1) to 𝐵=(𝑥2,𝑦2) a point 𝑃=(𝑥,𝑦)
 falls on you need to compute the value:-
𝑑=(𝑥−𝑥1)(𝑦2−𝑦1)−(𝑦−𝑦1)(𝑥2−𝑥1)
If 𝑑<0 then the point lies on one side of the line, and if 𝑑>0 then it lies on
 the other side. If 𝑑=0 then the point lies exactly line.
 
To see whether points on the left side of the line are those with positive
 or negative values compute the value for 𝑑 for a point you know is to the left of the line, such as (𝑥1−1,𝑦1) and then compare the sign with the point you are interested in.
'''













