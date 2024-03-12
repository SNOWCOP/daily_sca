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


def get_scf_ranges(delta, epsilon):  
    # get a dictionary with the SCF ranges for which you want to compute
    # the conditional probability
    SCF_1 = list(range(0,100,delta))
    SCF_2 = list(range(delta,100+delta,delta))
    
    # INFO about SCF variability --- load information
    scf_range_dic = {}
 
    for scf1,scf2 in zip(SCF_1,SCF_2):
        scf_l = scf1-epsilon
        scf_u = scf2+epsilon
        if scf_l < 0:
            scf_l = 0
        if scf_u > 100:
            scf_u=100
        key = str(scf_l) + '_' + str(scf_u)
            
        scf_range_dic[key] = (scf1,scf2)
        
    return scf_range_dic


def upsample(array1, array2):
    
    # Pixel size of the original image
    init_pixel_size_x = array1.coords['x'][-1] - array1.coords['x'][-2]
    init_pixel_size_y = array1.coords['y'][-1] - array1.coords['y'][-2]
    
    final_pixel_size_x = array2.coords['x'][-1] - array2.coords['x'][-2]
    final_pixel_size_y = array2.coords['y'][-1] - array2.coords['y'][-2]
    
    # new spatial coordinates
    xmin = array1.coords['x'].min() - init_pixel_size_x/2 + final_pixel_size_x/2
    xmax = array1.coords['x'].max() + init_pixel_size_x/2 - final_pixel_size_x/2
    # segno invertito perchè res y è negativa
    ymin = array1.coords['y'].min() + init_pixel_size_y/2 - final_pixel_size_y/2
    ymax = array1.coords['y'].max() - init_pixel_size_y/2 + final_pixel_size_y/2
    
    coord_x = np.linspace(start=xmin, 
                          stop=xmax,
                          num=array2.shape[-1])
    coord_y = np.linspace(start=ymin, 
                          stop=ymax,
                          num=array2.shape[-2])
    factor = 25
    upsampled = array1.values.repeat(factor, axis=-1).repeat(factor, axis=-2)
    upsampled = np.flip(upsampled,axis=2)
    # Keep the original time coordinates.
    coord_t = array2.coords['t'].values

    # Add a new dimension for time.
    grid = xr.DataArray(upsampled, 
                                      dims=[ 'bands', 't','y', 'x'], 
                                      coords=dict(t=coord_t, x=coord_x, y=coord_y))
    return grid

