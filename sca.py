#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 17:45:14 2023

@author: vpremier
"""

from pathlib import Path
import json
import openeo
from shapely.geometry import shape
import geopandas as gpd
import matplotlib.pyplot as plt
import os 
from utils import extent_from_shp
import leafmap.foliumap as leafmap
import xarray as xr
from openeo.udf import execute_local_udf

# set working directory
curr_dir=os.getcwd()
base_path = curr_dir+os.sep+"results"

if not os.path.exists(base_path):
    os.mkdir(base_path)

# read a shapefile with the AOI
shp_path = r'/mnt/CEPH_PROJECTS/PROSNOW/research_activity/Senales/auxiliary/boundaries/SenalesCatchment/SenalesCatchment.shp'
# shp_path = r'/home/vpremier/Documents/openEO/test.shp'
bounds = extent_from_shp(shp_path)

catchment_outline = gpd.read_file(shp_path)
bbox = catchment_outline.bounds.iloc[0]

# authentication
eoconn = openeo.connect("https://openeo-dev.vito.be")
eoconn.authenticate_oidc()
eoconn.describe_account()

# load the Copernicus fractional snow cover collection
scf = eoconn.load_collection(
    "FRACTIONAL_SNOW_COVER",
    spatial_extent  = {'west':bbox[0],'east':bbox[2],'south':bbox[1],'north':bbox[3],'crs':4326},
    temporal_extent=['2023-08-02','2023-08-07'],
    bands=["FSCTOC"]
)

# first test : apply function to calculate scf 
scf.download(base_path + os.sep + 'scf_0.nc')

# Apply the UDF to a cube.
binarize = openeo.UDF.from_file('udf-binarize.py', context={"from_parameter": "context"})

scf_test = scf.apply(process=binarize, context={"snowT": 20})

scf_test.download(base_path + os.sep + 'scf_binary.nc')

vv
# check job in https://editor.openeo.org/
scf_test = scf_test.save_result(format='netCDF')
job = scf_test.create_job(title='scf_test7')
job.start_job()
# results = job.get_results()
# results.download_files('test_scf3.nc')

sys.exit()
"""
DEBUGGING
"""

#execute the udf locally
path=r'/home/vpremier/Documents/openEO/results/scf_0.nc'
array = xr.open_dataarray(path,decode_coords="all")

# path2=r'/home/vpremier/Documents/openEO/test_sc.nc/openEO.nc'
# array2 = xr.open_dataarray(path2,decode_coords="all")
# smoothing_udf = Path('downscaling_udf.py').read_text()
output = execute_local_udf(binarize, path, fmt='netcdf')


output.get_datacube_list()[0].get_array().plot()
scf_test.plot()
# output.download('scf_test.nc')
# output.metadata()
# udf = output._datacube_list(output.to_dict())