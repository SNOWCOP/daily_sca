#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 22 20:16:37 2025

@author: vpremier
"""

import openeo
import geopandas as gpd
from openeo.processes import and_, is_nan, is_valid, not_, is_nodata, or_, if_

from utils_gapfilling import *
from utils import *


# ==============================
# User Configuration Section
# ==============================


#credentials CDSE
username = "valentina.premier@eurac.edu"
psw = "Openeo_290691"

# openEO backend
backend = 'https://openeo.dataspace.copernicus.eu/'

# out directory
outdir = r'/home/vpremier/OEMC/CODE/daily_sca_reconstruction/results'

# period to be downloaded
startdate = '2023-08-02'
enddate = '2023-08-15'

# cloud probability 
cloud_prob = 100

# extent
west=631800.
south=5167700.
east=655800.
north=5184200.

# resolution
res = 20.

# Ratio betweeen the size of a LR and a HR pixel, e.g., 500 m and 20 m.
pixel_ratio = 25 
# non-valid values
codes = [205, 210, 254, 255] 
nv_value = 205
# Threshold of non valid HR pixels allowed within a LR pixel [%]
nv_thres = 10 

# delta and epsilon: are used to define the SCF ranges. 
# The delta defines the steps, while epsilon represents a security buffer
delta = 10
epsilon = 10


# ==============================
# Authentication
# ==============================

eoconn = openeo.connect(backend, auto_validate=False)
eoconn.authenticate_oidc()
eoconn.describe_account()




# ==============================
# Load collections
# ==============================

scl = eoconn.load_collection(
    "SENTINEL2_L2A",
    temporal_extent=[startdate, enddate],
    spatial_extent={'west': west,
                    'east': east,
                    'south': south,
                    'north': north,
                    'crs':32632}, 
    bands=['SCL'],
    max_cloud_cover=cloud_prob,
)

modis = eoconn.load_stac("https://stac.eurac.edu/collections/MOD10A1v61",
                      temporal_extent = ["2023-01-20","2023-01-21"])
# modis.download('results/modis.nc')


# ==============================
# Get the snow cover information
# ==============================

# get the snow mask
snow = calculate_snow(scl)


# resample to 20 m spatial resolution
snow_rsmpl = snow.resample_spatial(resolution=res, 
                                 projection=32632,
                                 method = "near")
# snow_rsmpl.download('results/snow_rsmpl.nc')

# mask with valid and snow pixels
total_mask = create_mask(snow_rsmpl)

# get SCF
average = total_mask.resample_cube_spatial(modis, method="average")

# SCF [0-1]
scf_lr = average.band("snow")/average.band("valid")
scf_lr = scf_lr.multiply(100.)
scf_lr = scf_lr.apply(lambda x: if_(is_nan(x), 205, x))


# Compute the minimum SCF (SCF that you would obtain if the non valid pixels are replaced with 0-snow free)
# SCF min and max are not used here (but will be used in another step of the workflow..)
scf_min = average.band("snow")
scf_max = 1 - average.band("valid") + average.band("snow")

# scf_min.download('results/scf_min.nc')
# scf_max.download('results/scf_max.nc')


# Replace pixels with non valid data < threshold with 205
valid_threshold = 1-nv_thres/100

nv_mask = (average.band("valid")<=valid_threshold)*1.0
scf_lr_masked = scf_lr.mask(nv_mask, replacement=nv_value)

# scf_lr_masked.download('results/scf_lr_masked.nc')


# ==============================
# Conditional probabilities
# ==============================
scf_dic = get_scf_ranges(delta, epsilon)

# Loop for different snow cover fraction ranges
for i, key in enumerate(scf_dic):
    # range with a buffer - to be considered for the CP computation
    scf_1 = int(key.split('_')[0])
    scf_2 = int(key.split('_')[1])
    print(f'Computing CP by considering {scf_1}<SCF<={scf_2}')
    
    # define the mask scf_1 < scf <= scf_2
    mask_scf = ((scf_lr_masked > scf_1) & (scf_lr_masked <= scf_2)) *1.0
    
    # upsample again to HR
    mask_scf_hr = mask_scf.resample_cube_spatial(snow)

    # add mask with the snow pixels 
    mask_cp_snow = (mask_scf_hr & total_mask.band("snow")) * 1.0    

    
    # sum of all the snow pixels over time
    sum_cp_snow = mask_cp_snow.reduce_dimension(reducer="sum", 
                                                dimension="t")
    
    
    # mask of all the scf occurences over time
    occurences = mask_scf_hr.reduce_dimension(reducer="sum", 
                                                dimension="t")
    
    # conditional probabilities
    cp = sum_cp_snow/occurences
    

    
    # range to be used when performing the downscaling
    label_cp = 'cp_' + str(scf_dic[key][0]) + '_' + str(scf_dic[key][1])
    label_occur = label_cp.replace('cp','occur')
    
    # save information
    cp_renamed = cp.drop_dimension('bands').add_dimension(type="bands",name="bands",label=label_cp)
    occur_renamed = occurences.add_dimension(type="bands",name="bands",label=label_occur)

    if i==0:
        cp_stac = cp_renamed
        occur_stac = occur_renamed
    else:
        cp_stac = cp_stac.merge_cubes(cp_renamed)
        occur_stac = occur_stac.merge_cubes(occur_renamed)
    
    
# cp_stac.download("cp_stac.nc")
# occur_stac.download("occur_stac.nc")
job = cp_stac.create_job(title="cp_stac")
job.start()

results = job.get_results()