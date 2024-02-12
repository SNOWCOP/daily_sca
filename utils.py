import geopandas as gpd
from decimal import Decimal,ROUND_FLOOR
import numpy as np


def extent_from_shp(shp):
    #read the shapefile
    gdf = gpd.read_file(shp)
    
    # convert crs (otherwise may result in an error)
    if not gdf.crs == 'EPSG:4326':
        gdf = gdf.to_crs('EPSG:4326')  
        
    # Extract the Bounding Box Coordinates
    bounds = gdf.total_bounds

    # round 
    b1 = float(Decimal(str(bounds[0])).quantize(Decimal('.01'), rounding=ROUND_FLOOR))
    b2 = float(Decimal(str(bounds[1])).quantize(Decimal('.01'), rounding=ROUND_FLOOR))
    b3 = round(bounds[2],2)
    b4 = round(bounds[3],2)

    rounded_bounds = np.array([b1, b2, b3, b4])
    # get the center of the AOI
    lat = (bounds[1] + bounds[3])/2
    lon = (bounds[0] + bounds[2])/2
    return rounded_bounds
