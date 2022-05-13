from shapely import wkt
from shapely.geometry import Polygon
import matplotlib.pyplot as plt
import configs

''' Any spatial/non-spatial post processing after spatial query 
For instance, create a simple plot of polygons to validate their
intersection.

Note: This need not be distributed
'''

def plot_intersecting_polygons(polygons):
    #plt.rcParams["figure.figsize"] = [7.00, 3.50]
    plt.rcParams["figure.autolayout"] = True
    
    for p_pair in polygons:
        p1 = wkt.loads(p_pair[0])
        x, y = p1.exterior.xy
        plt.plot(x, y, c="red")
        
        p1 = wkt.loads(p_pair[1])
        x, y = p1.exterior.xy
        plt.plot(x, y, c="red")
            
    plt.savefig(configs.output_dir + '/fig.png')
