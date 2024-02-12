#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 12:32:19 2023

@author: vpremier
"""

from openeo.udf import XarrayDataCube

import numpy as np
import xarray as xr


def apply_datacube(cube: XarrayDataCube,
                   context: dict) -> XarrayDataCube:
    """
    If a pixel value is greater or equal then a threshold, will set up as 
    100. If smaller, will be set up as 0.
    
    FSCTOC (Copernicus) cloud values are set as 205 -> this value is kept
    0 (no snow) is set as no data
    """
    
    snowT = context['snowT']
    
    array = cube.get_array()
    

    
    # valid pixel, no cloud, SCF between snowT and 100 : set as 100 
    condition1 = array.notnull() & (array >= snowT) & (array <= 100) & (array!=205)
    array = xr.where(condition1, 100, array)

    # valid pixel, no cloud, SCF between 0 and snowT : set as 0 
    condition2 = array.isnull() | ((array >= 0) & (array < snowT) & (array!=205))
    array = xr.where(condition2, 0, array)

    
    
    array = array.astype(np.int16)

    return XarrayDataCube(array)
