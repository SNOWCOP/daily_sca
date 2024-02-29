#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 16:10:08 2023

@author: vpremier
"""
import xarray
from openeo.udf import XarrayDataCube
from openeo.udf.debug import inspect
from openeo.metadata import CollectionMetadata, SpatialDimension
import numpy as np

def apply_metadata(metadata: CollectionMetadata,
                   context: dict) -> CollectionMetadata:
    """
    Modify metadata according to up-sampling factor
    """
    new_dimensions = metadata._dimensions.copy()
    for index, dim in enumerate(new_dimensions):
        if isinstance(dim, SpatialDimension):
 
            new_dim = SpatialDimension(name=dim.name,
                                       extent=dim.extent,
                                       crs=dim.crs,
                                       step=dim.step / 2.0)
            new_dimensions[index] = new_dim

    updated_metadata = metadata._clone_and_update(dimensions=new_dimensions)
    return updated_metadata



def fancy_upsample_function(array: np.array, factor: int = 2) -> np.array:
    # assert array.ndim == 3
    return array.repeat(factor, axis=-1).repeat(factor, axis=-2)
    
    
    


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    cubearray: xarray.DataArray = cube.get_array()
    inspect(cubearray, "cubearray evok")
    inspect(cubearray.coords, "cubearray.coords evok")
    inspect(cubearray.data.ndim, "cubearray.data.ndim evok")

    # Pixel size of the original image
    init_pixel_size_x = cubearray.coords['x'][-1] - cubearray.coords['x'][-2]
    init_pixel_size_y = cubearray.coords['y'][-1] - cubearray.coords['y'][-2]

    predicted_array = fancy_upsample_function(cubearray.data, 2)

    # new spatial coordinates
    xmin = cubearray.coords['x'].min() - init_pixel_size_x/2 + init_pixel_size_x/4
    xmax = cubearray.coords['x'].max() + init_pixel_size_x/2 - init_pixel_size_x/4
    # segno invertito perchè res y è negativa
    ymin = cubearray.coords['y'].min() + init_pixel_size_y/2 - init_pixel_size_x/4
    ymax = cubearray.coords['y'].max() - init_pixel_size_y/2 + init_pixel_size_x/4
    
    
    coord_x = np.linspace(start=xmin, 
                          stop=xmax,
                          num=predicted_array.shape[-1])
    coord_y = np.linspace(start=ymin, 
                          stop=ymax,
                          num=predicted_array.shape[-2])
    
    # Keep the original time coordinates.
    coord_t = cube.get_array().coords['t'].values

    # Add a new dimension for time.
    predicted_cube = xarray.DataArray(predicted_array, 
                                      dims=['t', 'bands', 'y', 'x'], 
                                      coords=dict(t=coord_t, x=coord_x, y=coord_y))
    
    return XarrayDataCube(predicted_cube)









