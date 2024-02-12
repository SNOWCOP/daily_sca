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


    

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    

    array: xr.DataArray = cube.get_array()


    inspect(message="Print array")
    inspect(array)
    inspect(message="Print array dims")
    inspect(array.dims)
    inspect(message="Print array coords")
    inspect(array.coords)
        


    return XarrayDataCube(array)

    
    

            

    

 