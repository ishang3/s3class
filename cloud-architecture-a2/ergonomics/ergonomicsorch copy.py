import json
import math


class Ergonomics:
    def __init__(self,):
        self.window_size = 30
        self.values = []
        self.sum = 0
        

    def getAngle(self,a, b, c):
        ang = math.degrees(math.atan2(c[1] - b[1], c[0] - b[0]) - math.atan2(a[1] - b[1], a[0] - b[0]))
        return ang + 360 if ang < 0 else ang

    def process(self, value):
        self.values.append(value)
        self.sum += value
        if len(self.values) > self.window_size:
            self.sum -= self.values.pop(0)
        return float(self.sum) / len(self.values)

    def initialize_(self,):

        count = 0
        for value in data:
            val = data[value]
            curr_points = []
            for point in val:
                curr_points.append(point)
                if point == '5':
                    val5x, val5y = val['5'][0], val['5'][1]

                if point == '11':
                    val11x, val11y = val['11'][0], val['11'][1]

                if point == '13':
                    val13x, val13y = val['13'][0], val['13'][1]

                if point == '7':
                    val7x, val7y = val['7'][0], val['7'][1]

                if point == '8':
                    val8x, val8y = val['8'][0], val['8'][1]

                if point == '9':
                    val9x, val9y = val['9'][0], val['9'][1]

                if point == '10':
                    val10x, val10y = val['10'][0], val['10'][1]

                if point == '6':
                    val6x, val6y = val['6'][0], val['6'][1]

                if point == '12':
                    val12x, val12y = val['12'][0], val['12'][1]

                if point == '14':
                    val14x, val14y = val['14'][0], val['14'][1]

                if all(x in curr_points for x in ['6', '12', '14']) == True:
                    print(self.getAngle((val6x, val6y), (val12x, val12y), (val14x, val14y)))


                # if all(x in curr_points for x in ['5', '11', '13']) == True:
                #     angle = 360 - self.getAngle((val5x, val5y), (val11x, val11y), (val13x, val13y))
                #     print(angle,self.process(angle),'ishan')




