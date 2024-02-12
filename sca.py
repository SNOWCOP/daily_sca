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
import leafmap.foliumap as leafmap
import xarray as xr
from openeo.udf import execute_local_udf
from openeo.processes import and_, is_nan

# import glob
# import shutil
# fileList = glob.glob(r'/mnt/CEPH_PROJECTS/PROSNOW/SENTINEL-2/Copernicus_SCF/zipped_folders/*32TPT*')
# for f in fileList:
#     shutil.move(f, f.replace('zipped_folders','32TPT'))
    

# set working directory
curr_dir=os.getcwd()
base_path = curr_dir+os.sep+"results"

if not os.path.exists(base_path):
    os.mkdir(base_path)

# read a shapefile with the AOI
shp_path = r'/mnt/CEPH_PROJECTS/PROSNOW/research_activity/Senales/auxiliary/boundaries/SenalesCatchment/SenalesCatchment.shp'
# shp_path = r'/home/vpremier/Documents/openEO/test.shp'
# bounds = extent_from_shp(shp_path)

catchment_outline = gpd.read_file(shp_path)
bbox = catchment_outline.bounds.iloc[0]

# authentication
eoconn = openeo.connect("https://openeo-dev.vito.be")
eoconn.authenticate_oidc()
eoconn.describe_account()

# load the Copernicus fractional snow cover collection
scf = eoconn.load_collection(
    "FRACTIONAL_SNOW_COVER",
    spatial_extent  = {'west':bbox[0],
                       'east':bbox[2],
                       'south':bbox[1],
                       'north':bbox[3],
                       'crs':4326},
    temporal_extent=['2023-08-02','2023-08-15'],
    bands=["FSCTOC"]
)


# first test : apply function to calculate scf 
# scf.download(base_path + os.sep + 'scf_0.nc')

scf_rsmpl = scf.resample_spatial(resolution=20, projection=32632,
                                        method = "near")
scf_bbox = scf_rsmpl.filter_bbox(west=631910, south=5167310, east=655890, 
                                 north=5184290, crs=32632)

# scf_bbox.download(base_path + os.sep + 'scf_rsmp.nc')

# Resampling: should be done on the modis extent

binarize = openeo.UDF.from_file('udf-binarize.py', 
                                context={"from_parameter": "context"})
scf_binary = scf_bbox.apply(process=binarize, 
                            context={"snowT": 20})
scf_binary_renamed = scf_binary.rename_labels(dimension="bands",
                                              target=["scf"])
# scf_binary.download(base_path + os.sep + 'scf_binary.nc')


# Alternative: band math
# Problem with the unsigned bits
# scf_test = 100.0 * (scf >= 20) * (scf <= 100) + 205.0 * (scf == 205)
# scf_test = is_nan(scf_test)*0
# scf_test.download(base_path + os.sep + 'scf_binary2.nc')



# def mask_valid(data):
#     binary = data.array_element("scf")
#     mask = is_nan(binary)   
    
#     return mask
 
# mask = scf_binary_renamed.apply(mask_valid)
# scf_binary_masked = scf_binary_renamed.mask(mask,replacement=0)



cube_updated = scf_binary.apply_neighborhood(
    lambda data: data.run_udf(udf=udf_code, runtime='Python-Jep', context=dict()),
    size=[
        {'dimension': 'x', 'value': 128, 'unit': 'px'},
        {'dimension': 'y', 'value': 128, 'unit': 'px'}
    ], overlap=[])

cube_updated.download(base_path + os.sep + 'scf_test.nc')

ccc

mask = (scf_binary >= 0)*1.0
aggregation = Path('udf-test.py').read_text()
test = scf_binary.apply(process=aggregation)


test = scf_binary.apply_neighborhood(
    lambda data: data.run_udf(udf=aggregation, runtime='Python-Jep'),
    size=[
        {'dimension': 'x', 'value': 600, 'unit': 'px'},
        {'dimension': 'y', 'value': 500, 'unit': 'px'}
    ], overlap=[])

# test.download(base_path + os.sep + 'scf_test.nc')
cp_test = test.save_result(format='netCDF')
job = cp_test.create_job(title='test4')
job.start_job()
ss
# valid_mask = scf_binary > 0
# scf_binary_nonan = scf_binary.mask(~valid_mask ,replacement = 0) 


# scf_binary_masked = scf_binary_renamed.mask(mask,replacement=0)
# scf_binary_masked.download(base_path + os.sep + 'scf_bin_masked.nc')


# aggregation = openeo.UDF.from_file('udf-scf.py', context={"from_parameter": "context"})
aggregation = Path('udf-scf.py').read_text()

# scf_aggregated = scf_binary.apply(process=aggregation, 
#                                   context={"pixel_ratio": 25})
# scf_aggregated = scf_binary.apply_dimension(dimension="t",
#                                             process=aggregation, 
#                                             context={"pixel_ratio": 25})
scf_aggregated = scf_binary.apply_neighborhood(
    lambda data: data.run_udf(udf=aggregation, runtime='Python-Jep'),
    size=[
        {'dimension': 'x', 'value': 600, 'unit': 'px'},
        {'dimension': 'y', 'value': 500, 'unit': 'px'}
    ], overlap=[])
# scf_aggregated = scf_binary.apply_neighborhood(aggregation, 
#                                   size=[
#                                           {'dimension': 'x', 'value': 400, 'unit': 'px'},
#                                           {'dimension': 'y', 'value': 50, 'unit': 'px'}
#                                       ], 
#                                   overlap=[])




scf_aggregated_renamed = scf_aggregated.rename_labels(dimension="bands",target=["aggregated"])
scf_aggregated.download(base_path + os.sep + 'scf_aggregated6.nc')


hhhh
merged_cube = scf_binary_renamed.merge_cubes(scf_aggregated)

# get the CP

scf_1 = 90.
scf_2 = 100.
def mask_compute(data):
    # band = aggregated.array_element(0)

    aggregated = data.array_element("aggregated")
    binary = data.array_element("scf")
    combined_mask = and_(aggregated>scf_1, aggregated<=scf_2)
    combined_mask = and_(combined_mask, binary==100)
    
    return combined_mask
 
mask_scf_snow = merged_cube.apply(mask_compute, replacement=0)




snow_sum = (mask_scf_snow*1.0).reduce_dimension(reducer = "sum", dimension = "t")
# valid_sum = (mask_scf_valid*1.0).reduce_dimension(reducer = "sum", dimension = "t")

# cp = snow_sum/valid_sum
# cp.download(base_path + os.sep + 'cp.nc')
# # 


dd


# check job in https://editor.openeo.org/
cp_test = test.save_result(format='netCDF')
job = cp_test.create_job(title='test')
job.start_job()
results = job.get_results()
results.download_files('scf_bin3.nc')

sys.exit()
"""
DEBUGGING
"""

#execute the udf locally
path=r'/home/vpremier/Documents/openEO/results/scf_binary.nc'
array = xr.open_dataset(path,decode_coords="all")


aggregation = openeo.UDF.from_file('udf-scf.py', 
                                   context={"from_parameter": "context"})
myudf = Path('udf-scf.py').read_text()
output = execute_local_udf(aggregation, path, fmt='netcdf')


output.get_datacube_list()[0].get_array().plot()
np.shape(output.get_datacube_list()[0])
array[1].plot()
# output.download('scf_test.nc')
# output.metadata()
# udf = output._datacube_list(output.to_dict())