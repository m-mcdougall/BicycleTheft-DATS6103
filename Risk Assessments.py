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

import cartopy.crs as ccrs
from cartopy.io.img_tiles import OSM
from matplotlib import cm
from matplotlib.colors import Normalize

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


def calculate_risk(thefts_in_radius_df):

    
    """
    
    Need to weight the risk factors
    
    Recency of theft
    Distance from theft
    If locks were circumvented (Increase threat if yes, decreas if no, neutral where it's not specified)
    If yes, what kind of lock (rank in terms of security, higher rank = more threat)
    Make note of bike cage, but don't effect weighting
    
    """
    
    
    threat_df=thefts_in_radius_df.copy()
    
    #All the locking options available in the theft reporting section
    #1: Not secured/Unknown, 2:Secured, 3:Well Secured
    
    security_rankings={1:['None', 'Not locked','Other'],
                       2:['U-lock and cable', 'Chain with padlock','U-lock', 'Cable lock'],
                       3:['Heavy duty bicycle security chain', 'Two U-locks'],
                       }
    
    security_rankings_inv={}
    for key in security_rankings:
        for i in security_rankings[key]:
            security_rankings_inv[i] = key
    
    security_threat = threat_df['Locking description'].replace(security_rankings_inv)
    
    
    #Distance
    
    dist_threat = 10/threat_df['Distance']
    
    
    #Recency
    
    from datetime import datetime as datetimeOverlap
    days_since_theft= (datetimeOverlap.now()-threat_df['DateTime']).dt.days
    days_threat = 10/(0.01+days_since_theft)
    
    
    
    
    
    
    overall_threat = security_threat * dist_threat * days_threat
    
    
    overall_threat.sum()
    
    threat_df["Lock_Status"] = security_threat
    threat_df["Time_Status"] = days_since_theft
    threat_df["Threat_Overall"] =overall_threat
    
    return threat_df

#%%


street_address= 'George Washington University'

#street_address= '10925 Baltimore Ave, Beltsville, MD 20705'

#street_address= '7450 Wisconsin Ave, Bethesda, MD 20814'

street_address='Boston'

 
    
user_loc=get_user_loc(street_address)    

theft_nearby=thefts_in_radius(user_loc, main_df, search_radius=2)

theft_nearby=calculate_risk(theft_nearby)





#%%


def theft_plot_transform(theft_nearby_df, labels=False):
    """
    Transforms the theft nearby dataframe into colours/radii 
    """

    
    #Set a normalization for assigning the colour gradient
    #A little larger than the cut-off, since the yellow in the chosen 'autumn' gradient end is hard to see.
    norm = Normalize(vmin=0, vmax=185)
    
    #Useses the normalization to generate the labels for plot
    if labels==True:
        return [ cm.autumn(norm(i),) for i in [30,60,90,180]]
    
    #Filter unneeded values
    theft_plot=theft_nearby_df.copy()
    theft_plot=theft_plot.filter(['Bike ID','Latitude',
                                  'Longitude', 'DateTime', 'Distance', 'Lock_Status',
                                  'Time_Status', 'Threat_Overall'])
    
    #Create a count of all bikes stolen at unique locations and dates
    loc_counts = theft_plot.groupby(['Longitude', 'Latitude', 'Time_Status']).size().reset_index(name='Theft_Count')   
    lock_counts = theft_plot.groupby(['Longitude', 'Latitude', 'Time_Status'])["Lock_Status"].mean().reset_index()
    loc_counts["Lock_Status"] = lock_counts["Lock_Status"].apply(lambda x: int(round(x,0))) 
    
    #Set default values
    theft_plot['Color']= 'Grey'
    theft_plot['Number']= 1
    
    #Loop through
    for i in range(theft_plot.shape[0]):
        theft = theft_plot.iloc[i, :]
        
        #If recent, change colour along gradient, else leave as grey
        if theft["Time_Status"]<=180:
            theft_plot['Color'].iloc[i] = cm.autumn(norm(theft["Time_Status"]),)
        
        #Compare to thefts at that exact location within a week of the theft
        #Used to mark out repeated hits, or bike cage break-ins (steal many bikes)
        count_here = loc_counts[(loc_counts.Longitude == theft.Longitude) &
                                (loc_counts.Latitude == theft.Latitude) &
                                ((loc_counts.Time_Status <= theft.Time_Status+7) & (loc_counts.Time_Status >= theft.Time_Status-7))]
        theft_plot['Number'].iloc[i] = count_here['Theft_Count'].sum()
        theft_plot["Lock_Status"].iloc[i] = int(round(count_here["Lock_Status"].mean()))
        
    
    #Drop duplicate entries - already recorded the number of thefts, do not need the extras
    theft_plot = theft_plot.drop_duplicates(['Longitude', 'Latitude','Number']) 
    
    #Put an upper limit on the number of thefts that get plotted
    #If you don't, a single large bike cage theft (~10 bikes) will fill the entire map
    theft_plot['Number']=theft_plot['Number'].apply(lambda x: 3 if x >= 3 else x)    

    return theft_plot





def plot_nearby_thefts(theft_nearby, user_loc, scale='small'): 
    """
    V2: Integrating colourmap and rdius coding for the map
    Needs this fix: https://stackoverflow.com/questions/57531716/valueerror-a-non-empty-list-of-tiles-should-be-provided-to-merge-cartopy-osm

    """    

    
    size_adj= {'small':[0.5,16],
               'medium':[1,15], 
               'large':[2, 14],
               }
    
    if scale.lower() not in size_adj.keys():
        raise ValueError(f"Please make sure your scale is set to Small, Medium or Large.")

    size=size_adj[scale.lower()]
    
    if theft_nearby.shape[0]>0:
        #Get the radii and colours for all thefts
        theft_plot = theft_plot_transform(theft_nearby)
    
    #Initiate the figure
    #fig = plt.figure(figsize=(12,10))
    
    fig = plt.figure(figsize=(18,17))
    
    #The base map will be from Open Street Maps
    imagery = OSM()
    
    #Use the OSM projection for positioning lat/longitude points
    ax = plt.axes(projection=imagery.crs, )
    
    # Set the extent - center on the user location +/- 1 mile
    long_adj = 1/54.6 * size[0]
    lat_adj  = 1/69 * size[0]
    ax.set_extent( (user_loc.longitude-long_adj, user_loc.longitude+long_adj,
                    user_loc.latitude-lat_adj, user_loc.latitude+lat_adj))
    
    
    #Plot the user location
    plt.plot(user_loc.longitude,user_loc.latitude, color='red', markersize=30, marker='*',transform=ccrs.Geodetic() )
    plt.plot(user_loc.longitude,user_loc.latitude, color='white', markersize=18, marker='*',transform=ccrs.Geodetic() )
    
    if theft_nearby.shape[0]>0:
        #Plot all nearby bike thefts
        for i in range(theft_plot.shape[0]):
            plt.plot(theft_plot['Longitude'].iloc[i],theft_plot['Latitude'].iloc[i], color=theft_plot['Color'].iloc[i], markersize=theft_plot['Number'].iloc[i] * 35, marker='o', alpha=0.4, transform=ccrs.Geodetic() )
            plt.plot(theft_plot['Longitude'].iloc[i],theft_plot['Latitude'].iloc[i], color=theft_plot['Color'].iloc[i], markersize=10, marker='o',transform=ccrs.Geodetic() )
            
    
    # Add the imagery to the map.
    zoom = size[1] #15 is best for 1 mile 
    ax.add_image(imagery, zoom )
    plt.title(f'Bike thefts within {size[0]} Mile of {user_loc.address.split(",")[0]}')
    

    
    
    
    plt.show()
    
    



#%%


def search_near_me(user_street_address, scale='small', main_df=main_df):

    
    size_adj= {'small':0.5,'medium':1,'large':2,}
    
    if scale.lower() not in size_adj.keys():
        raise ValueError(f"Please make sure your scale is set to Small, Medium or Large.")
    
    
    user_loc=get_user_loc(user_street_address)    

    theft_nearby=thefts_in_radius(user_loc, main_df, search_radius=size_adj[scale.lower()])
    
    theft_nearby=calculate_risk(theft_nearby)

    plot_nearby_thefts(theft_nearby, user_loc, scale)
    
    return theft_nearby
#%%

x=search_near_me(user_street_address="Austin, TX", scale='small', )

#%%

qy=theft_plot_transform(0, labels=True)

for i in range(len(y)):
    plt.plot(i,i, color=qy[i], markersize=18, marker='o')
    print(i)
plt.show()












