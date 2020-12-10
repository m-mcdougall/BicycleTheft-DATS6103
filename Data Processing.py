# -*- coding: utf-8 -*-



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


pd.set_option('display.max_columns', 10)


wd=os.path.abspath('C://Users//Mariko//Documents//GitHub//BicycleTheft-DATS6103')
os.chdir(wd)


#%%
x=[pd.read_csv('./Data/'+i, index_col=0) for i in os.listdir('./Data')]

df_base=pd.concat(x).reset_index(drop=True)

#%%

def clean_bike_IDs(df):
    
    #Clean the IDs - some older pages had a location string attatched
    df_cleaned = df['Bike ID'].astype(str)
    df_cleaned = df_cleaned.str.split('?', n=2, expand=True)
    df_cleaned = df_cleaned.iloc[:, 0].astype(int)
    
    df['Bike ID'] = df_cleaned
    
    return df


def clean_address(df):

    
    #Remove Edmonton, Alberta. Not sure how it consistantly gets in.
    df=df[~df['Location'].str.contains('Edmonton') ]
    
    #Comma placement is standardized on the site, can be used to parse the locations
    df['Full Address'] = df['Location'].str.count(',')
    
    #Remove all the single name locations as they cannot be parsed, eg "Florida"
    df = df[df['Full Address'] > 0]
    
    #Convert to binary if there is a full address or just city
    df['Full Address'] = df['Full Address'] > 1
    
    #Extract city and state
    df['City']=df['Location'].str.rsplit(',', n=2).str[-2].str.strip()
    df['State']=df['Location'].str.extract(r'(\b[A-Z][A-Z]\b)')
    
    #Drop rows that didn't parse state correctly
    df=df.dropna()
    
    return df


def add_datetime(df_in):
    """
    Adds a datetime column to the dataframe by parseing the input date string
    Returns the modified dataframe
    """
    
    df=df_in.copy()
    
    datetime=[]
    
    for i in df['Date stolen']:
        if i == 'None':
            datetime.append(np.nan)
        else:
            date=i.split('.')
            date=date[0]+date[1].zfill(2)+date[2].zfill(2)
            datetime.append(date)
    
    df['DateTime']=pd.to_datetime(datetime, format='%Y%m%d')

    return df

#%%
df_base=clean_bike_IDs(df_base)

df_base=clean_address(df_base)

df_base=add_datetime(df_base)

#%%


df=df_base.copy()

df['Latitude']='None'
df['Longitude']='None'
df['Address']='None'



from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="BikeThefts")

problems=[]

for i in tqdm(range(df.shape[0])):
    if df['Full Address'].iloc[i] == True:
        try:
            address=df['Location'].iloc[i]
            location = geolocator.geocode(address)
            
            df['Latitude'].iloc[i]  = location.latitude
            df['Longitude'].iloc[i] = location.longitude
            df['Address'].iloc[i]   = location.address
        
            #Required interval between requests for Nominatim's TOS
            time.sleep(1)
        except:
            problems.append(df['Location'].iloc[i])


#%%
            
#df.to_csv(f'./Data/Locations.csv')            
problems_df=pd.DataFrame(problems)
#problems_df.to_csv(f'./Data/Locations_problems.csv') 
#%%
"""
Problems

1) Apartments - DONE
2) intersections
3) W vs West
4) st. breaks it

"""

#%%

"""
Remove Apartments

"""

test = ["39 Symphony Rd, Unit C, Boston, MA 02115",
        "1600 Hillcrest drive apt 2, Manhattan, KS 66502",
        "3272 Fuhrman Ave E #218, Seattle, WA 98102",
        '333 Sunol St, Unit 313, San Jose, CA 95126',
        "2140 nw Kearney St apt414, Portland, OR 97210",
        "3477 Lily Way, 107, San Jose, CA 95134", 
        "1420 South Alamo, San Antonio, TX 78210"]


def remove_apartments(address_list):
    """
    Removes several common appartment labels that interfere with the location search
    Includes "Apt ___", 'Unit ____,  # ____, and just the number between two commas (eg, ', 201,)
    """
    
    unsolved_index=list(range(len(address_list)))
    
    
    def pattern_loop(pattern, address_list, unsolved_list):
        """
        Applies a regex pattern to the list of addresses, and removes
        matching elements from the addresses if found, returning the modified addresses
        and the list of addresses that still need to be fixed
        
        """
    
        internal_fixed=[]

    
        #Search for the regex pattern in each entry
        for addr_index in unsolved_list:
            addr_str=address_list[addr_index]
            match= pattern.search(addr_str.title())
            try:
                #If a match was found, eliminate the matched substring
                substring=match[0]
                addr_new=addr_str.title().replace(substring, '')
                
                #Replace the string in the list, and track the index
                address_list[addr_index] = addr_new
                internal_fixed.append(addr_index)
                
                print(addr_str)
                print(addr_new)
                print()
    
            except:
                pass
        
        #Remove elements from the unsolved list that were solved - must do at the end or the iteration breaks
        if len(internal_fixed)>0:
            unsolved_list=list(filter(lambda i: i not in internal_fixed, unsolved_list))
    
        return address_list, unsolved_list
    
    
    print(f'\nApartments with the word Apartment or Unit')
    print(f'---------------------\n')
    
    #Removes the apartments that have the prefix "Apt" or "Unit"
    pattern=re.compile("\W([Aa]pt|[Uu]nit)[\w|\W]+?[,]")
    address_list, unsolved_index = pattern_loop(pattern, address_list, unsolved_index)


    
    print(f'\nApartments with the prefix #_____')
    print(f'---------------------\n')
    
    #Removes the apartments that have the prefix #_____
    pattern=re.compile("[#][\w|\W]+?[,]")
    address_list, unsolved_index = pattern_loop(pattern, address_list, unsolved_index)

    
    print(f'\nApartments with format , #### , ')
    print(f'---------------------\n')
    
    #Removes the apartments that have just the number between two commas
    pattern=re.compile("[,][\W|0-9]+?[,]")
    address_list, unsolved_index = pattern_loop(pattern, address_list, unsolved_index)

    
    print(f'\nApartments with the word Ste (studio/suite)')
    print(f'---------------------\n')
    
    #Removes the apartments that have just the number between two commas
    pattern=re.compile("\s[Ss]te[\s,.][\w|\W]+?[,]")
    address_list, unsolved_index = pattern_loop(pattern, address_list, unsolved_index)


    return address_list


#addresses_fixed = remove_apartments(problems)



#%%

test_fixed = remove_apartments(problems)

#%%


"""
Shortened directions

Try lengthening, then if not fixed, removing
"""

test = ["1071 W Martin Luther King Jr Blvd, Los Angeles, CA 90007",
        "747 W Cornelia , Chicago, IL 60657",
        "650 E Bidwell, Folsom, CA 95630",
        '18100 95th street NE, Redmond, WA 98052',
        "7905A SE Powell Blvd, Portland, OR 97206",
        "226 W Laurel, Fort Collins, CO 80521", 
        "3445 NE Williams, Portland, OR 97212"]



def modify_directions(address_list):
    """
    Both lengthens and removes common directional abbrviations (eg NE for NorthEast)
    Returns both lists - use lengthened first, then shortened for maximum specificity
    """
    
    def directions_pattern_loop(pattern, address_list):
        """
        Applies a regex pattern to the list of addresses, and removes
        matching elements from the addresses if found, returning the modified addresses
        and the list of addresses that still need to be fixed
        
        """
    
        internal_long=[]
        internal_short=[]
    
    
        #Search for the regex pattern in each entry
        for addr_str in address_list:
            match= pattern.search(addr_str.title())
            try:
                #If a match was found, eliminate the matched substring
                substring=match[0]
                mod_substring = substring.upper()
    
                #Replace all abbreviations with the full word            
                for initial, full in {"N":"North", "S":"South", "E":"East", "W":"West",}.items():
                    mod_substring = mod_substring.replace(initial.upper(), full)
                
                #Replace the original substring with the modified version
                addr_new=addr_str.title().replace(substring, mod_substring)
                internal_long.append(addr_new)
                
                print(addr_str)
                print(addr_new)
                
                #Also append a version without the direction, in case the first doesn't work
                addr_new=addr_str.title().replace(substring, ' ')
                internal_short.append(addr_new)
    
                print(addr_new)
                print()
    
            except:
                #If match not found, then add to list regardless
                internal_long.append(addr_str)
                internal_short.append(addr_str)
    
        return internal_long, internal_short
    
    
    print(f'\nAddresses with Directional Abbreviations')
    print(f'---------------------\n')
    
    #Create three lists, one with the directions lengthened, one with them cropped out, and a continuing list
    pattern=re.compile("\s[NESWnesw]{1,2}\s")
    out_fixed_long, out_fixed_short = directions_pattern_loop(pattern, address_list)
    
    
    return out_fixed_long, out_fixed_short



#%%%


df_loc = df_base.copy()


df_loc["Modified Location"] = remove_apartments(df_loc["Location"].tolist())
df_loc["Modified Location"], df_loc["Modified Shortened Location"] = modify_directions(df_loc["Modified Location"].tolist())







#%%


from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="BikeThefts")
location = geolocator.geocode("6510 Tucker Avenue, McLean, VA 22101")
print(location.address)

print((location.latitude, location.longitude))


#%%

from geopy.geocoders import Photon

geolocator = Photon(user_agent="BikeThefts")
location = geolocator.geocode("Alameda & Bannock, Denver, CO 80223")
print(location.address)

print((location.latitude, location.longitude))



#%%

def find_locations(df_in):
    """
    Insert dataframe with modified location columns, will query photon and nomanatim to get locations
    Returns modified dataframe and list of problem addresses for further invsetication.
    """
    
    df_loc_found=df_in.copy()
    
    
    df_loc_found['Latitude']='None'
    df_loc_found['Longitude']='None'
    df_loc_found['Address']='None'
    
    
    from geopy.geocoders import Nominatim
    from geopy.geocoders import Photon
    
    geolocator_nominatim = Nominatim(user_agent="BikeThefts")
    geolocator_photon = Photon(user_agent="BikeThefts")
    
    problems=[]
    nominatim_interval=time.perf_counter()
    
    for i in tqdm(range(df_loc_found.shape[0])):
        
        #Only search full addresses - the location of a city center is not useful.
        if df_loc_found['Full Address'].iloc[i] == True:
            
            #First, try Photon - has the best success rate
            try:
                address=df_loc_found['Modified Location'].iloc[i]
                
                location = geolocator_photon.geocode(address)
                
                df_loc_found['Latitude'].iloc[i]  = location.latitude
                df_loc_found['Longitude'].iloc[i] = location.longitude
                df_loc_found['Address'].iloc[i]   = location.address
            
    
            except:
                #Then try Nominatim
                try:
                    #Required interval between requests for Nominatim's TOS
                    t = time.perf_counter()
                    if (t - nominatim_interval) < 1:
                        time.sleep((t - nominatim_interval))
                    
                    address=df_loc_found['Modified Location'].iloc[i]
                    
                    location = geolocator_nominatim.geocode(address)
                    
                    df_loc_found['Latitude'].iloc[i]  = location.latitude
                    df_loc_found['Longitude'].iloc[i] = location.longitude
                    df_loc_found['Address'].iloc[i]   = location.address
                    
                    #Reset the nominatim countdown
                    nominatim_interval=time.perf_counter()
                    
                except:
                    #Need to reset the nominatim countdown anyways
                    nominatim_interval=time.perf_counter()
                    
                    #Then try Photon on the abbreviated 
                    try:
                        address=df_loc_found['Modified Shortened Location'].iloc[i]
                        
                        location = geolocator_photon.geocode(address)
                        
                        df_loc_found['Latitude'].iloc[i]  = location.latitude
                        df_loc_found['Longitude'].iloc[i] = location.longitude
                        df_loc_found['Address'].iloc[i]   = location.address
            
                    
                    except:
                        #Don't try Nominatim for the final round - unlikely to work, and adds 1s/search
                        problems.append(df_loc_found['Modified Location'].iloc[i])
                        
    
    
    return df_loc_found, problems              

#%%
    

df_loc_found, problems = find_locations(df_loc)
#%%

"""
Save the database

"""

df_loc_found.to_csv(f'./Data/Main_Database.csv')


#%%

"""
Load the database from file

"""

loaded = pd.read_csv(f'./Data/Main_Database.csv', index_col=0, )# parse_dates=['Date']



#%%







#%%

"""
How to calculate distance efficiently
first filter all results such that either the lat or the long is = or < the maximum radial distance
(eg, another theft that happened at the exact same lat, and the farthest possible long)

then with this reduced dataset calculate the radial distance for each theft from the seach point

then filter by the set radial distance (sinc some will be longer due to a combo of the lat/long)

then you have the thefts w/in distance.


"""



















































