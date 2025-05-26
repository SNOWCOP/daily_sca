#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 15:18:53 2025

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



# resample to 20 m spatial resolution
snow_rsmpl = scl.resample_spatial(resolution=res, 
                                 projection=32632,
                                 method = "near")

scf_bbox = snow_rsmpl.filter_bbox(west=631900, 
                                 south=5167810, 
                                 east=655600, 
                                 north=5184190, 
                                 crs=32632)

# scf_bbox.download('scf_bbox.nc')
job = scf_bbox.create_job(title="scf_bbox")
job.start()


