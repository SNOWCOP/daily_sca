#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 12 14:08:02 2023

@author: vpremier
"""

import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import box


           
def get_matching_s2_cdse(date_start, date_end, username, psw, shp = None,
                         max_cc = 90, tile = None, filter_date = True):
    
    """Returns list of matching Sentinel-2 scenes for a selected period and
        for a specific area (defined from a shapefile). The username and
        password of your Copernicus Data Space Ecosystem account are required. 
        Please see
        
        https://dataspace.copernicus.eu/
            
        Parameters
        ----------
        date_start : str
            starting date
        date_end : str
            ending date
        shp : str 
            path to a shapefile with your area of interest. Any crs is accepted
        username : str
            username of your CDSE account
        psw : str
            password of your CDSE account
        max_cc : int, optional
            maximum cloud coverage. Default is 90%
        tile : str, optional
            specific tile to be downloaded
        filter_date : bool, optional
            whether to filter double dates, by keeping the last processing time
        
        Returns
        -------
        products : list
            list of the matching scenes
    """   

    # access to the Copernicus Dataspce ecosystem
    data = {
           "client_id": "cdse-public",
           "username": username,
           "password": psw,
           "grant_type": "password",
       }
    try:
        r = requests.post(
            "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
            data=data,
        )
        r.raise_for_status()
    except Exception as e:
        raise Exception(
            f"Access token creation failed. Reponse from the server was: {r.json()}"
        )
        
        
    if shp is None:  
        boundsdata=box(*[-180,-90,180, 90]).wkt
    else:
        # search by polygon, time, and CDSE query keywords
        gdf = gpd.read_file(shp)
        
        # convert crs (otherwise may result in an error)
        if not gdf.crs == 'EPSG:4326':
            gdf = gdf.to_crs('EPSG:4326') 
            
        boundsdata=box(*gdf.total_bounds).wkt
    


    data_collection = "S2MSI1C"

    # query
    if tile is None:
        query = ('').join([f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=",
                           "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType'",
                           " and att/OData.CSC.StringAttribute/Value eq '",
                           data_collection,
                           "')",
                           " and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover'",
                           " and att/OData.CSC.DoubleAttribute/Value lt ",
                            str(max_cc),
                            ") and OData.CSC.Intersects(area=geography'SRID=4326;",
                            boundsdata,
                            "') and ContentDate/Start gt ",
                            date_start,
                            "T00:00:00.000Z and ContentDate/Start lt ",
                            date_end,
                            "T00:00:00.000Z&$top=1000"])
    else:     
        query = ('').join([f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=",
                           "contains(Name,'",
                           tile,
                           "') and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType'",
                           " and att/OData.CSC.StringAttribute/Value eq '",
                           data_collection,
                           "')",
                           " and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover'",
                           " and att/OData.CSC.DoubleAttribute/Value lt ",
                            str(max_cc),
                            ") and OData.CSC.Intersects(area=geography'SRID=4326;",
                            boundsdata,
                            "') and ContentDate/Start gt ",
                            date_start,
                            "T00:00:00.000Z and ContentDate/Start lt ",
                            date_end,
                            "T00:00:00.000Z&$top=1000"])

    json = requests.get(query).json()
    
    products = pd.DataFrame.from_dict(json['value'])
    
     
    if filter_date:
        # keep last processing date ( or newest Processing Baseline Nxxxx)
        
        products["commonName"] = [('_').join([f.split('_')[i] for i in[0,1,2,4,5]]) for f in products['Name']]
        

        products = products.sort_values(by='Name')
        products_fltd = products.drop_duplicates(subset='commonName', keep='last')
        
        products = products_fltd
        products_fltd = products_fltd.reset_index(drop=True, inplace=True)
    
    print('Found %i Sentinel-2 scenes from %s to %s with maximum cloud coverage %i%%' 
          % (len(products),date_start, date_end, max_cc))
    
    
    
    return products




           
            
           
            

  
