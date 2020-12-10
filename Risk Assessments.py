# -*- coding: utf-8 -*-
"""
How to calculate distance efficiently
first filter all results such that either the lat or the long is = or < the maximum radial distance
(eg, another theft that happened at the exact same lat, and the farthest possible long)

then with this reduced dataset calculate the radial distance for each theft from the seach point

then filter by the set radial distance (sinc some will be longer due to a combo of the lat/long)

then you have the thefts w/in distance.


"""
#%%

import os
import re
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import concurrent.futures as cf
from tqdm import tqdm
from geopy.geocoders import Nominatim
from geopy.geocoders import Photon

pd.set_option('display.max_columns', 10)
pd.options.mode.chained_assignment = None 


wd=os.path.abspath('C://Users//Mariko//Documents//GitHub//BicycleTheft-DATS6103')
os.chdir(wd)


#%%

"""
Load the database from file

"""

main_df = pd.read_csv(f'./Data/Main_Database.csv', index_col=0,  parse_dates=['DateTime'])



#%%


def get_user_loc(street_address):
    """
    Takes a user input and convert it to a geopy location
    """
    
    #Load the geolocators    
    geolocator_nominatim = Nominatim(user_agent="BikeThefts")
    geolocator_photon = Photon(user_agent="BikeThefts")
    
    #First check photon - most reliable
    location = geolocator_photon.geocode(street_address)
    
    #Check if Photon found the address
    if location is None: 
        #Photon failed - attempt Nominatim
        location = geolocator_nominatim.geocode(street_address)
    
    #Both have failed, return to user for error checking.    
    if location is None:
        raise ValueError(f"\n Address not found. Please ensure that you have entered a valid address.\n Previous Entry: {street_address}\n ")
        
    return location





#%%


def thefts_in_radius(user_loc, main_df, search_radius=10):
    """
    Finds nearby theft locations within a set search radius(in miles)
    Note: raidus may vary depending on longitude of user - users closer to the equator will have slightly larger radii
    """

    #Latitude and longitude have different conversion raitios - must be handled seperately
    lat_radius = 1/69 * search_radius
    long_radius = 1/54.6 * search_radius
    
    #Set these as vairables to increase readablility
    user_lat=user_loc.latitude
    user_long=user_loc.longitude
    
    #Filter the main database to only include known latitudes and longitudes
    limited_df = main_df[main_df["Latitude"] != 'None']
    limited_df["Latitude"] =limited_df["Latitude"].astype(float)
    limited_df["Longitude"] =limited_df["Longitude"].astype(float)
    
    #Filter to limit data to those thefts within the maxium radii
    limited_df = limited_df[
            ((limited_df["Latitude"]>=(user_lat-lat_radius)) & (limited_df["Latitude"]<=(user_lat+lat_radius))) &
            ((limited_df["Longitude"]>=(user_long-long_radius)) & (limited_df["Longitude"]<=(user_long+long_radius)))
            ]
    
    
    #Now, need to calculate how far that theft is from the user location
    #(The above may be farther away than the search value, if they are at maximum lat/long, but have additional long/lat)
    
    #Triangulate using the pythagorian formula, converting distances back into miles
    limited_df['Distance'] = ((user_long-limited_df["Longitude"])*54.6)**2 + ((user_lat-limited_df["Latitude"])*69)**2
    limited_df['Distance'] = np.sqrt(limited_df['Distance'])
    
    #Filter results down to those within the search radius
    in_radius_df=limited_df[limited_df["Distance"] <= search_radius]
    
    return in_radius_df
#%%


#street_address= 'George Washington University'

street_address= '10925 Baltimore Ave, Beltsville, MD 20705'
    
user_loc=get_user_loc(street_address)    

theft_nearby=thefts_in_radius(user_loc, main_df, search_radius=10)



#%%













































































