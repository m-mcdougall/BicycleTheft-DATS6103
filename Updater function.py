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


"""
Load the database from file

"""

main_df = pd.read_csv(f'./Data/Main_Database.csv', index_col=0,  parse_dates=['DateTime'])





#%%



def update_get_urls(page_url, main_df):
    """
    Query a page of the stolen bikes, and check if urls are from US, and if any reports
    have already been downloaded. 
    Return a flag indicating if a repeat occured.
    """
    
    #Flag for tracking if bikes already present
    reoccur_flag=0
    
    #Download the page
    page = requests.get(page_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    #Get the table of bike thefts, and look through by theft
    results = soup.find_all("div", {"class":"bike-information multi-attr-lists"})
    
    clean_urls=[]
    
    #Check to make sure the location is present in the theft report
    for result in results:
        theft_location= result.find(string=re.compile('Location')).parent.next_sibling
        
        #Theft reports always terminate in country code, and if location is only "US", there is no - .
        if '- US' in theft_location:
            
            bike_url=result.find('a').get('href')
            
            #Check if bike already in database, and if not capture URL
            try: 
                bike_ID= int(bike_url.split('/bikes/')[1])
    
                if bike_ID in main_df['Bike ID'].values:
                    reoccur_flag = 1
                else:
                    clean_urls.append(bike_url)
            except:
                pass


    return clean_urls, reoccur_flag





def update_bikes_downloader(main_df):
    """
    Download all the bikes per search page that have a US location listed
    Does not download non-us or non location specified thefts
    """
    
    reoccur_flag=0
    
    
    #First page has slightly different url
    page_url = 'https://bikeindex.org/bikes?&stolenness=all'
    
    #Get page URLs, as long as they are new
    url_list, reoccur_flag = update_get_urls(page_url, main_df)
    
    #Download the batch of urls
    df_page=download_batch_url(url_list)
    
    i=2
    while reoccur_flag == 0:
        
        print(f'........\nNow downloading page {i}\n........')
        page_url = 'https://bikeindex.org/bikes?page='+str(i)+'&stolenness=all'
    
        #Get page URLs, as long as they are new
        url_list, reoccur_flag = update_get_urls(page_url, main_df)
        
        #Download the batch of urls
        try:
            df_page=pd.concat([df_page, download_batch_url(url_list)])
        except:
            print(f'........\nError on page {i}\n........')
    

        i+=1
    
    #Check if there are any downloaded files first
    if df_page.shape[0] >0 :
        
        #For privacy, change the report number to a binary value
        df_page['Police report'] = df_page['Police report'] != 'None'
        df_page['Police report'] = df_page['Police report'].replace({True:'Reported', False:'None'})

    
    return df_page





def update_bikes(main_df):
    """
    Downloads new bike thefts and updates the database of bike thefts
    Stops downloading when it encounters previously downloaded thefts.    
    """
    
    df_new = update_bikes_downloader(main_df)

    #Only do processing if new files were downloaded    
    if df_new.shape[0] > 0:
                
        #Clean IDS and Addresses
        df_new=clean_bike_IDs(df_new)
        df_new=clean_address(df_new)
        df_new=add_datetime(df_new)
        
        #Remove common appartment number issues and elongate directions
        df_new["Modified Location"] = remove_apartments(df_new["Location"].tolist())
        df_new["Modified Location"], df_new["Modified Shortened Location"] = modify_directions(df_new["Modified Location"].tolist())
        
        #Find the exact lat and longitude
        df_new, problems = find_locations(df_new)
    
        #Concatinate with the main database and save
        main_df = pd.concat([main_df, df_new])
        main_df.to_csv(f'./Data/Main_Database.csv')

    print(f'\n ------------- \n\n Your Database is now up to date.\n\n ------------- \n')

    return main_df





df=update_bikes(main_df)






























