import math



def midpoint(x1,y1,x2,y2):
    """
    calculates the mid point coordinates between 2 coords
    """
    return ((x1 + x2)/2, (y1 + y2)/2)


def calculate_distance(a,b):
    x1,y1 = a 
    x2,y2 = b
    dist = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    return dist

    


