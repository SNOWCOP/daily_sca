#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 16:12:16 2023

@author: vpremier
"""

from openeo.udf import XarrayDataCube
from openeo.udf.debug import inspect
from openeo.metadata import CollectionMetadata

import numpy as np
import xarray as xr



def apply_metadata(input_metadata:CollectionMetadata, 
                   context:dict) -> CollectionMetadata:

    pixel_ratio = context["pixel_ratio"]
    
    xstep = input_metadata.get('x','step')
    ystep = input_metadata.get('y','step')
    new_metadata = {
          "x": {"type": "spatial", 
                "axis": "x", 
                "step": xstep/pixel_ratio, 
                "reference_system": 32632},
          "y": {"type": "spatial", 
                "axis": "y", 
                "step": ystep/pixel_ratio, 
                "reference_system": 32632},
          "t": {"type": "temporal"}
    }
    return CollectionMetadata(new_metadata)


def scf(array: np.array, 
        pixel_ratio: int = 20,
        scf_max: bool = False,
        scf_min: bool = False,
        codes: list = [205, 210, 254, 255],
        nv_thres: int = 40) -> np.array:
    
    """
    This function calculates the snow cover fraction from a snow cover map which 
    is given as input as a np.array
    
    Parameters
    ----------
    array : np.array 
        representing the snow cover map
    pixel_ratio : int, optional
        number of pixels to be aggregated to obtain the new resolution 
        (e.g. 20x20)
    scf_max : bool, optional
        if True, consider all the non valid pixels as snow
    scf_min : bool, optional
        if True, consider all the non valid pixels as snow free
    codes : list, optional
        list of the codes assigned as no data value
    nv_thres : int, optional
        max number of pixels which can be no data. If this number is matched or 
        exceeded, the aggregated pixel is assigned as no data
        
    Returns
    -------
    aggregated : np.array
        array with the aggregated fractional snow cover
    
    """
    #x and y dimensions
    assert array.ndim == 2
    
    
    # number of columns and rows of the output aggregated array
    nrows = int(np.shape(array)[0]/pixel_ratio)
    ncols = int(np.shape(array)[1]/pixel_ratio)
    
    #initialize aggregated array
    aggregated = np.ones(shape=(nrows, ncols)) * 255
    
    scf_correct = not(scf_min) and not(scf_max)
    
    # iterate over rows
    y_j = 0
    x_i = 0
    for j in range(0, nrows):     
        # reset column counter
        x_i = 0             
        # iterate over columns
        for i in range(0, ncols):

            # read the slice of the scene matching the current
            # estimator pixel
            data_ij = array[y_j:y_j + pixel_ratio,x_i: x_i + pixel_ratio]
            data_ij[np.isnan(data_ij)] = 0
            
            # check if the pixels are valid
            if any([erc in data_ij for erc in codes]):
                nv_sum = sum([np.sum(data_ij == erc) for erc in codes])
                
                if scf_min:
                    aggregated[j, i] = np.sum(data_ij[data_ij <= 100]) / data_ij.size 
                    
                if scf_max:
                    aggregated[j,i] = (np.sum(data_ij[data_ij <= 100]) + \
                                      nv_sum*100) / data_ij.size 
                
                
                if scf_correct and nv_sum < nv_thres:
                    # calculate snow cover fraction
                    aggregated[j, i] = np.sum(data_ij[data_ij <= 100]) / (data_ij.size - nv_sum)
            else: 
                
                aggregated[j,i] = np.sum(data_ij) / data_ij.size
            # advance column counter by number of high resolution pixels
            # contained in one low resoution pixels
            x_i += pixel_ratio
        
        # advance row counter by number of high resolution pixels
        # contained in one low resoution pixels
        y_j += pixel_ratio
        
    return aggregated
    

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    
    # pixel_ratio = context["pixel_ratio"]
    # scf_max = context["scf_max"]
    # scf_min = context["scf_min"]
    # codes = context["codes"]
    # nv_thres = context["nv_thres"]
    # D = np.array ([datetime.strptime(str(d)[0:10],'%Y-%m-%d') for d in dat.t.values])
    pixel_ratio = 25
    array: xr.DataArray = cube.get_array()

    # We make prediction and transform numpy array back to datacube

    # Pixel size of the original image
    init_pixel_size_x = array.coords['x'][-1] - array.coords['x'][-2]
    init_pixel_size_y = array.coords['y'][-1] - array.coords['y'][-2]

    
        
    # for date in array.coords['t']:
    #     cubearray: xarray.DataArray = array['t'==date].copy()
    cubearray: xr.DataArray = cube.get_array().copy()
    print(cubearray.coords)
    # if cubearray.data.ndim == 3 and cubearray.data.shape[0] == 1:
    cubearray = cubearray[0][0]
    # print(cubearray.coords)
    # print(cubearray.data.shape)
    predicted_array = scf(cubearray.data, 
                          pixel_ratio=pixel_ratio)
    
    # NEW COORDINATES
    xmin = array.coords['x'].min() - 10 + 250
    xmax = array.coords['x'].max() + 10 - 250
    ymin = array.coords['y'].min() - 10 + 250
    ymax = array.coords['y'].max() + 10 - 250
    coord_x = np.linspace(start = xmin, 
                          stop = xmax,
                          num = predicted_array.shape[-2])
    coord_y = np.linspace(start = ymin, 
                          stop = ymax,
                          num = predicted_array.shape[-1])
    
    predicted_cube = xr.DataArray(predicted_array, 
                                  dims=['x', 'y'], 
                                  coords=dict(x=coord_x, y=coord_y))

    return XarrayDataCube(predicted_cube)

    
    

            

    

 