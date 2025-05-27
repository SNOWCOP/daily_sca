#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 10:08:55 2024

@author: vpremier
"""

import openeo
from openeo.processes  import if_, is_nan
from typing import List, Optional

import logging
_log = logging.getLogger(__name__)

# def cloud_mask_compute(data):
    
#     band = data.band("SCL")
#     # include also 10 cirrus, 2 and 3 cloud shadows
#     combined_mask = (band==7) | (band==8) | (band==9)
    
#     return combined_mask*1.0

def calculate_cloud_mask(scl: openeo.DataCube) -> openeo.DataCube:
    """
    Calculate cloud mask from SCL data.
    Args:
        scl (openeo.datacube.DataCube): SCL data cube.
    Returns:
        openeo.datacube.DataCube: Cloud mask data cube.
    """
    _log.info(f'calculating cloud mask')

    classification = scl.band("SCL")
    binary = (classification == 7) | (classification == 8) | (classification == 9) 
    binary = binary.add_dimension(name="bands", label="clouds", type="bands")
    return binary





def calculate_snow(scl: openeo.DataCube) -> openeo.DataCube:
    """
    Calculate snow mask from SCL data.
    Args:
        scl (openeo.datacube.DataCube): SCL data cube.
    Returns:
        openeo.datacube.DataCube: Cloud mask data cube.
    """
    _log.info(f'calculating snow mask')

    classification = scl.band("SCL")
    clouds = ((classification == 7) | (classification == 8) | (classification == 9) ) * 1.0
    snow = (classification == 11) * 100
    snow = snow.mask(clouds, replacement = 205)
    snow = snow.add_dimension(name="bands", label="snow", type="bands")
    return snow


def calculate_ndsi(s2: openeo.DataCube) -> openeo.DataCube:
    """
    Calculate NDSI.
    Args:
        s2 (openeo.datacube.DataCube): S2 data cube.
    Returns:
        openeo.datacube.DataCube: NDSI data cube.
    """
    _log.info(f'calculating ndsi')

    B03 = s2.band("B03")
    B11 = s2.band("B11")

    ndsi = (B03 - B11)/(B03+B11)
    ndsi = ndsi.add_dimension(name="bands", label="ndsi", type="bands")
    return ndsi


def salomonson(ndsi: openeo.DataCube) -> openeo.DataCube:
    """
    Apply the approach of Salomonson and Appel (2006) to get fractional snow cover
    Args:
        ndsi (openeo.datacube.DataCube): ndsi data cube.
    Returns:
        openeo.datacube.DataCube: SCF data cube.
    """
    
    NDSI = ndsi.band("ndsi")
    scf = (- 0.01  + 1.45 * NDSI) * 100

    scf = scf.mask(scf>100,replacement=100)  
    scf = scf.mask(scf<0,replacement=0)  

    scf = scf.apply(lambda x: if_(is_nan(x), 205, x))
    
    scf = scf.add_dimension(name="bands", label="scf", type="bands")

    return scf


def binarize(scf: openeo.DataCube,
             snowT: Optional[int] = 20 ) -> openeo.DataCube:
    """
    Binarize the scf to 0 and 100 (205 clouds)
    Args:
        scf (openeo.datacube.DataCube): ndsi data cube.
    Returns:
        openeo.datacube.DataCube: binary SCF
    """
    
    scf = scf.band("scf")
    mask_100 = (scf >= snowT) & (scf <= 100) & (scf!=205)
    scf = scf.mask(mask_100,replacement=100)  

    mask_0 = (scf < snowT) & (scf <= 100) & (scf!=205)
    scf = scf.mask(mask_0,replacement=0)  
    
    scf = scf.add_dimension(name="bands", label="scf", type="bands")

    return scf


def create_mask(snow: openeo.DataCube) -> openeo.DataCube:
    """
    Create two masks: mask with valid pixels and mask with 
    pixels classified as snow.
    Args:
        snow (openeo.datacube.DataCube): snow data cube (0:snow free, 100: snow, 205: clouds).
    Returns:
        openeo.datacube.DataCube: mask
    """
    
    snow = snow.band("snow")
    
    mask_valid = (snow <= 100 )*1.0
    mask_valid = mask_valid.add_dimension(name="bands", label="valid", type="bands")
        
    mask_snow = (snow == 100 )*1.0 
    mask_snow = mask_snow.add_dimension(name="bands", label="snow", type="bands")

    mask = mask_valid.merge_cubes(mask_snow)

    return mask

