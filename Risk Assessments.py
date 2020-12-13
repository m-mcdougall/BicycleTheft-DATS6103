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
import seaborn as sns
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






def scrub_states(main_df):
    """
    Check to see if all states were accurately generated originally, and do a 
    more through parse if they are incorrect. Remove entries if they cannot be fixed.
    Return corrected dataframe
    """
    state_abbrev=['AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL', 'GA', 'GU', 'HI', 'ID', 'IL', 'IN',
            'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM',
            'NY', 'NC', 'ND', 'MP', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VI', 'VA',
            'WA', 'WV', 'WI', 'WY']
    
    #Seperate out the entries that did not parse correctly originally
    misparsed=main_df[(~main_df.State.isin(state_abbrev)) | (main_df.State == 'NE')]
    
    #Manually extract the states from the user entered locations
    misparsed['State']=misparsed.Location.str.split(',').str[-1].str.strip().str.split(' ').str[0]
    #Fiter out entries that are not states, even with the re-filter
    fixed = misparsed[misparsed.State.isin(state_abbrev)]
    
    #Re-merge into the main dataframe
    main_df=pd.concat([main_df[(main_df.State.isin(state_abbrev) & (main_df.State != 'NE'))], fixed])
    
    return main_df





def thefts_in_radius(user_loc, main_df, search_radius=10, return_nonspecifics=False):
    """
    Finds nearby theft locations within a set search radius(in miles)
    Note: raidus may vary depending on longitude of user - users closer to the equator will have slightly larger radii
    If return non-specifics is true, will also return all the places that don't have a specific address.
    This is helpful for the total number of thefts in the city.
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
    
    #nonspecific location processing
    if return_nonspecifics == True:
        non_specifics = main_df[main_df["Latitude"] == 'None']
        #Make sure that the city is in the correct state (using one of the filtered state results)
        non_specifics = non_specifics[non_specifics.State == limited_df.State.iloc[0] ]
        non_specifics['Distance'] = search_radius
        
        in_radius_df=pd.concat([in_radius_df, non_specifics])
    
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
    lock_time = security_threat * days_threat 

    
    threat_df["Lock_Status"] = security_threat
    threat_df["Time_Status"] = days_since_theft
    threat_df["Threat_Overall"] =overall_threat
    threat_df["Lock_Time"] =lock_time
    
    return threat_df

#%%
main_df=scrub_states(main_df)

street_address= 'George Washington University'

#street_address= '10925 Baltimore Ave, Beltsville, MD 20705'

#street_address= '7450 Wisconsin Ave, Bethesda, MD 20814'

#street_address='Boston'

 
    
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
        try:
            theft_plot["Lock_Status"].iloc[i] = int(round(count_here["Lock_Status"].mean()))
        except:
            pass
        
    
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

x=search_near_me(user_street_address="3030 14th St NW, Washington, DC 20009", scale='small', )


#%%


city='Washington'

user_loc=get_user_loc(city)    

theft_nearby=thefts_in_radius(user_loc, main_df, search_radius=10)

theft_nearby=theft_nearby[theft_nearby["City"] == city]

theft_nearby=calculate_risk(theft_nearby)




def plot_city_thefts(theft_nearby, city): 
    """
    V2: Integrating colourmap and rdius coding for the map
    Needs this fix: https://stackoverflow.com/questions/57531716/valueerror-a-non-empty-list-of-tiles-should-be-provided-to-merge-cartopy-osm

    """    

    
    if theft_nearby.shape[0]>0:
        #Get the radii and colours for all thefts
        theft_plot = theft_plot_transform(theft_nearby)
    
    #Initiate the figure
    #fig = plt.figure(figsize=(12,10))
    
    fig = plt.figure(figsize=(15,19))
    
    #The base map will be from Open Street Maps
    imagery = OSM()
    
    #Use the OSM projection for positioning lat/longitude points
    ax = plt.axes(projection=imagery.crs, )
    
    # Set the extent - center on the user location +/- 1 mile
    long_adj = 1/54.6 * 5
    lat_adj  = 1/69 * 5
    
    #Center the plot on the mean theft value - Sometimes the default "city center" is far from town
    mean_lat=theft_plot.Latitude.mean()
    mean_long=theft_plot.Longitude.mean()
    
    ax.set_extent( (mean_long-long_adj, mean_long+long_adj,
                    mean_lat-lat_adj, mean_lat+lat_adj))
    
    
    #Plot the user location
    #plt.plot(user_loc.longitude,user_loc.latitude, color='red', markersize=30, marker='*',transform=ccrs.Geodetic() )
    #plt.plot(user_loc.longitude,user_loc.latitude, color='white', markersize=18, marker='*',transform=ccrs.Geodetic() )
    
    if theft_nearby.shape[0]>0:
        #Plot all nearby bike thefts
        for i in range(theft_plot.shape[0]):
            plt.plot(theft_plot['Longitude'].iloc[i],theft_plot['Latitude'].iloc[i], color=theft_plot['Color'].iloc[i], markersize=theft_plot['Number'].iloc[i] * 20, marker='o', alpha=0.4, transform=ccrs.Geodetic() )
            plt.plot(theft_plot['Longitude'].iloc[i],theft_plot['Latitude'].iloc[i], color=theft_plot['Color'].iloc[i], markersize=10, marker='o',transform=ccrs.Geodetic() )
            
    
    # Add the imagery to the map.
    zoom = 13 #15 is best for 1 mile 
    ax.add_image(imagery, zoom )
    plt.title(f'Bike thefts in {city}')
    

    
    
    
    plt.show()
    


plot_city_thefts(theft_nearby, city)


#%%

"""
Making the legend for the plots - needs work
"""



labels=theft_plot_transform(theft_nearby, labels=True)+['Grey']
label_text=['<1 month', '2 Months', '3 Months', '4 Months', '>4 Months']



ax2 = ax.twinx()
#The base map will be from Open Street Maps
plt.plot(1,1)
legend_ax = fig.add_axes([0.91, 0.33, 0.01, 0.36])
legend_ax.axes.get_xaxis().set_visible(False)
legend_ax.axes.get_yaxis().set_visible(False)

for i in range(len(labels)):
    legend_ax.plot(-100,-100, color=labels[i], label=label_text[i], markersize=10, marker='o',linestyle = 'None',)
   
legend_ax.plot(-100,-100, color='white', label='', markersize=10, marker='o',linestyle = 'None',)
   
for i in range(1,4):
    legend_ax.plot(-100,-100, color='Grey', label=str(i)+' Thefts',
                   markersize=i*25, marker='o',linestyle = 'None', alpha=0.4)

legend_ax.legend(borderpad=3, labelspacing =5, columnspacing =6)



#%%



"""
Look at relative risk - your location vs 1 mile vs city
"""

#Columbia heights
address = '3030 14th St NW, Washington, DC 20009'

address = 'George Washington University'
city='Washington'

user_loc=get_user_loc(address)    

#Local theft

theft_nearby=thefts_in_radius(user_loc, main_df, search_radius=0.5)
theft_nearby=calculate_risk(theft_nearby)
safety_nearby=theft_nearby["Lock_Time"].mean()

#1 Mile Theft

theft_local=thefts_in_radius(user_loc, main_df, search_radius=1)
theft_local=calculate_risk(theft_local)
safety_local=theft_local["Lock_Time"].mean()

#City theft

city_df=main_df[main_df["City"]==city]
theft_city = thefts_in_radius(user_loc, city_df, search_radius=25, return_nonspecifics=True)
theft_city=calculate_risk(theft_city)
safety_city=theft_city["Lock_Time"].mean()




def category_diff(diff):
    
    if diff < -0.1:
        text='significantly less'
    elif diff < -0.05:
        text='less'
    elif diff < 0:
        text='slightly less'
    elif diff < 0.05:
        text='slightly more'
    elif diff < 0.1:
        text='more'
    else:
        text='significantly more'

    return text
    


print(f'Your location is {category_diff(safety_local - safety_nearby)} safe than the local area.')
print(f'Your location is {category_diff(safety_city - safety_nearby)} safe than the city in general.')




per_nearby = (theft_nearby.shape[0]/theft_city.shape[0]) * 100
per_local = (theft_local.shape[0]/theft_city.shape[0]) * 100


print(f'\n')
print(f"Your local location accounts for {round(per_nearby, 1)}% of the city's bike thefts ({theft_nearby.shape[0]}/{theft_city.shape[0]}) (0.5 mile radius)")
print(f"The surrounding area accounts for {round(per_local, 1)}% of the city's bike thefts ({theft_local.shape[0]}/{theft_city.shape[0]}) (1 mile radius)")





locks_nearby = theft_nearby[theft_nearby['Locking description']!='None']
locks_nearby=locks_nearby['Lock_Status'].value_counts()


try:
    unsecured=locks_nearby[1]
except:
    unsecured=0

try:
    secured=locks_nearby[2]
except:
    secured=0

try:
    vsecured=locks_nearby[3]
except:
    vsecured=0

print(f'\n')
print(f'{secured + vsecured}/{theft_nearby.shape[0]} Stolen bikes in your area were reported as secured.' )
print(f'{vsecured}/{theft_nearby.shape[0]} Stolen bikes in your area were reported as highly secured.' )
print(f'{unsecured}/{theft_nearby.shape[0]} Stolen bikes in your area were reported as explicitly unlocked.' )




theft_nearby["Time_Status"].mean()
theft_nearby["Distance"].mean()

print(f'\n')
print(f'Closest stolen bike: {round(theft_nearby["Distance"].min(), 2)} miles.')
print(f'Mean distance to a stolen bike: {round(theft_nearby["Distance"].mean(), 2)} miles.')
print()
print(f'Most recent stolen bike: {round(theft_nearby["Time_Status"].min(), 0)} days.')
print(f'Mean time since a stolen bike: {round(theft_nearby["Time_Status"].mean(), 0)} days.')




"""
Reccomend parking where you are if current location is safer than local

Reccomend moving if local area is less dangerous than current location

If local or current location is more dnagerous than the city mean, alert user and advise 
the use of heavy security, and to keep an eye on their bike.


"""

print(f"\nOVERALL RECOMMENDATION:")

if  (safety_city-safety_nearby < -0.05) or (safety_city-safety_local < -0.05):
    print('\n     ---------------------------------------------\n')
    print('                        WARNING:')
    print(f"         Your current location is in a area prone to theft.")
    print(f'         High security locks are reccomended.')
    print('\n     ---------------------------------------------\n')

if safety_nearby - safety_local  < -0.02:
    print('This area is unusually high in theft compared to the local area.')
    print('Consider parking a 10-15 min walk away.')

else:
    print('Your current area has a similar theft risk to the local area.')

print('As always, exercise caution and lock your bike.')


#%%


#Label and merge the near, local and city datasets
theft_nearby["Category"] = "Nearby \n<0.5 Miles"
theft_local["Category"] = "Local \n<1 Mile"
theft_city["Category"] = "City"
theft_plot=pd.concat([theft_nearby,theft_local,theft_city])


#Generate a plot showing the distribution of days since theft
sns.boxenplot(data=theft_plot, x='Category', y='Time_Status',)# inner='box',cut=0)
plt.ylabel("Days Since Theft")
plt.ylim(0)
plt.xlabel('')
plt.title("Distribution of Days since Theft")
plt.show()



#Generate a plot comparing the bike security levels in the area vs the whole city
#Make the loc status categorical, and add an "Unknown" category
for i in range(theft_plot.shape[0]):
    if theft_plot['Locking description'].iloc[i] =='None':
        theft_plot['Lock_Status'].iloc[i] = 0

theft_plot['Lock_Status']=theft_plot['Lock_Status'].replace({0:'Unknown', 1:"Unlocked", 2:"Secured", 3:"Very Secured"})    

#Generate the countplot for the security data
sns.countplot(data=theft_plot,  hue='Lock_Status', x='Category')
plt.ylabel("# Bikes ")
plt.xlabel('')
plt.legend(title = "Security Level",  bbox_to_anchor=(1.35, .7))
plt.title("Bike Security Level in the Local Area")
plt.show()


##Show a swarmplot of all the individual bike theft risks
#sns.swarmplot(data=theft_plot, x='Category', y='Lock_Time')
#plt.ylabel("Bike Theft Risk Impact")
#plt.xlabel('')
#plt.title("Risk Impact per Bike Theft\n(Distance not Factored)")
#plt.show()


#%%







#%%%
"""

City facts
-----------
Thefts per year
Most thefts from one location
Most/least dangerous month for theft
Most dangerous day of the month (calander plot?)


State Facts
------------
Most stolen from city
Most dangerous month

Country Facts
-------------
Most dangerous state

"""


#%%

city='Washington'
state='DC'

city_df=main_df[(main_df.City == city) & (main_df.State == state)].copy()
city_df['Year'] = city_df['DateTime'].dt.year
city_df['Month'] = city_df['DateTime'].dt.month
city_df['Day'] = city_df['DateTime'].dt.day
city_df['Weekday'] = city_df['DateTime'].dt.dayofweek

count_year = city_df.groupby(['Year']).size().reset_index(name='Theft_Count')   
count_month = city_df.groupby(['Month']).size().reset_index(name='Theft_Count')   
count_day = city_df.groupby(['Day']).size().reset_index(name='Theft_Count')  
count_weekday = city_df.groupby(['Weekday']).size().reset_index(name='Theft_Count')   
count_weekday.Weekday.replace({0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday',
                               4:'Friday', 5:'Saturday', 6:'Sunday', }, inplace=True)  
        
count_month.Month.replace({	1 :'January',2:'February', 3:'March', 4:'April',
                              5:'May', 6:'June', 7:'July', 8:'August', 9:'September',
                              10:'October', 11:'November', 12:'December'}, inplace=True)

sns.lineplot(data=count_year, x='Year', y='Theft_Count')
plt.title(f'Total Annual Thefts in {city}')
plt.axvline(x=2013, color='orange',linestyle='dashed', label = 'Launch of BikeIndex')
plt.ylabel('# Bikes')
plt.legend(loc='lower right')
plt.show()

sns.lineplot(data=count_month, x='Month', y='Theft_Count',sort=False)
plt.title(f'Total Monthly Thefts in {city}')
plt.ylabel('# Bikes')
plt.show()

sns.lineplot(data=count_day, x='Day', y='Theft_Count')
plt.title(f'Thefts by Calandar Day in {city}')
plt.ylabel('# Bikes')
plt.show()

sns.lineplot(data=count_weekday, x='Weekday', y='Theft_Count',sort=False)
plt.title(f'Thefts by Week Day in {city}')
plt.ylabel('# Bikes')
plt.show()

#%%



def seperate_seasons(df_in, colors_only=False):
    """
    We'll use the meteorological season starts, rather than the astronomical starts
    This means that we use rounded-off months starting on the 1st
    """
    
    season_colours={'Winter':'#486BF9', 'Spring':'#48F97E', 'Summer':'#F9D648','Fall':'#F96648'}    
    seasons= season_colours.keys()
    seasons = [ele for ele in seasons for i in range(3)] 
    
    dates=[i for i in range(1,12+1)]
    dates=dates[-1:]+dates[:11]
    dates=dict(zip(dates,seasons))
    
    df_in["Season"]=df_in.Month.replace(dates)

    
    return df_in, season_colours



city_df_months, color_dict=seperate_seasons(city_df)
count_seasons = city_df.groupby(['Year', "Season"]).size().reset_index(name='Theft_Count') 

sns.lineplot(x="Year",y='Theft_Count', hue="Season", palette=color_dict, 
             data=count_seasons)
plt.show()




#fig = plt.figure(figsize=(10,5))
city_df_months, color_dict=seperate_seasons(city_df)
count_seasons = city_df.groupby(['Weekday', "Season"]).size().reset_index(name='Theft_Count') 
count_seasons.Weekday.replace({0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday',
                               4:'Friday', 5:'Saturday', 6:'Sunday', }, inplace=True)
        
sns.lineplot(x="Weekday",y='Theft_Count', hue="Season", palette=color_dict, 
             data=count_seasons, sort=False,)
plt.show()




#%%

def top_10_plot(main_df):
    
    """
    Show top10 cities for bike thefts reported
    
    """
    top10_thefts = main_df.groupby(["State", "City"]).size().reset_index(name='Theft_Count') 
    top10_thefts.sort_values("Theft_Count", inplace=True, ascending=False)
    
    top10_thefts=top10_thefts.head(10)
    top10_thefts["Labels"]= top10_thefts["City"] + ',\n'+ top10_thefts["State"]
    
    fig = plt.figure(figsize=(13,6))
    sns.barplot(data=top10_thefts, y='Labels', x='Theft_Count', palette = "Reds_d")# ['red']+['grey']*9)
    plt.title(f'Top 10 Cities for Reported Bike Thefts', fontsize=14)
    plt.xlabel('# Bikes Stolen')
    plt.ylabel('')
    plt.show()
    
    



top_10_plot(main_df)


#%%

def radial_plot(count_df, title_str):
        
    labels=count_df.iloc[:,0].to_list()
    stats=count_df.iloc[:,1].to_list()
    #stats=[stat -150 for stat in stats]
    
    
    #Get increments for the ticks
    tics = [50*i for i in range(10) if i*50<max(stats)]
    tics = tics[-3::] + [tics[-1]+50]
    
    angles=np.linspace(0, 2*np.pi, len(labels), endpoint=False)
    # close the plot
    stats=np.concatenate((stats,[stats[0]]))
    angles=np.concatenate((angles,[angles[0]]))
    
    fig=plt.figure(figsize=(6,6))
    ax = fig.add_subplot(111, polar=True)
    ax.plot(angles, stats, '-', linewidth=2, alpha=0.6)
    ax.fill(angles, stats, alpha=0.25)
    #ax.tick_params(pad=15)
    ax.set_thetagrids(angles * 180/np.pi, labels)
    
    plt.yticks(tics, [str(tic) for tic in tics], color="grey", size=8)
    
    plt.title(f'Bike Thefts per {title_str}', fontsize=14,y=1.08)
    
    
    ax.grid(True)
#%%
radial_plot(count_month, "Month")








