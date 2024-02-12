#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 11:58:13 2024

@author: vpremier
"""
from openeo.processes import and_


def get_cp_scf(merged_cube, delta, epsilon, pixel_ratio=20, nv_thres = 40, 
               snowT=0, outfld = None, save=False, codes=[205,210,255,254], 
               N_proc=-1, cut_nan = False):
    """
    This function calculates the snov cover fraction variability
    
    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with dates as Indices (format YYYY-MM-DD) and two columns 
        with S2A_fileName and LC8_fileName
    dem_path : str
        path to the corresponding DEM tif file
    SCF_1 : list
        list with the lower limits of the considered SCF ranges
    SCF_1 : list
        list with the upper limits of the considered SCF ranges
    pixel_ratio : int, optional
        number of pixels to be aggregated to obtain the new resolution 
        (e.g. 20x20)
    outfld = str, optional
        path to the folder where to save the outputs
    save : bool, optional
        whether to save the outputs as tif files
        
    Returns
    -------
    dic_scf_var : dict
        dictionary with the computed SCF variability maps
    dic_occur : dict
        dictionary with the computed number of occurences of the selected SCF
        ranges
    
    """
    
    # get a dictionary with the SCF ranges for which you want to compute
    # the conditional probability
    SCF_1 = list(range(0,100,delta))
    SCF_2 = list(range(delta,100+delta,delta))
    
    # INFO about SCF variability --- load information
    scf_range_dic = {}
    SCF_1_eps=[]
    SCF_2_eps=[]
    for scf1,scf2 in zip(SCF_1,SCF_2):
        scf_l = scf1-epsilon
        scf_u = scf2+epsilon
        if scf_l < 0:
            scf_l = 0
        if scf_u > 100:
            scf_u=100
        key = str(scf_l) + '_' + str(scf_u)
            
        scf_range_dic[key] = (scf1,scf2)

        SCF_1_eps.append(int(key.split('_')[0]))
        SCF_2_eps.append(int(key.split('_')[1]))




    
    
    
    
    dic_CP_scf = {}
    dic_occur = {}
    # for all the SCF values contained in the lists, compute SCF variability
    for SCF1, SCF2 in zip(SCF_1_eps,SCF_2_eps):
        key = str(SCF1) + '_' + str(SCF2)
        
        
        def mask_snow(data):   
            aggregated = data.array_element("aggregated")
            binary = data.array_element("scf")
            combined_mask = and_(aggregated>SCF1, aggregated<=SCF2)
            combined_mask = and_(combined_mask, binary==100)
            
            return combined_mask
        
        def mask_valid(data):   
            aggregated = data.array_element("aggregated")
            binary = data.array_element("scf")
            combined_mask = and_(aggregated>SCF1, aggregated<=SCF2)
            combined_mask = and_(combined_mask, binary<=100)
            
            return combined_mask
         
        mask_scf_snow = merged_cube.apply(mask_snow)
        mask_scf_valid = merged_cube.apply(mask_valid)
        
        
        snow_sum = (mask_scf_snow*1.0).reduce_dimension(reducer = "sum", 
                                                        dimension = "t")
        occurence = (mask_scf_valid*1.0).reduce_dimension(reducer = "sum", 
                                                          dimension = "t")

        CP_scf = snow_sum/occurence
        
        
        dic_CP_scf[key] = CP_scf
        dic_occur[key] = occurence
      
    

    
    return dic_CP_scf, dic_occur, scf_range_dic