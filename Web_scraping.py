# -*- coding: utf-8 -*-



#Going to want to extract the date stolen, the location, if the lock was circumvented, and the bike ID (address)



import os
import re
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import concurrent.futures as cf


pd.set_option('display.max_columns', 10)


wd=os.path.abspath('C://Users//Mariko//Documents//GitHub//BicycleTheft-DATS6103')
os.chdir(wd)


#%%




def download_batch_url(url_list):
    """
    Download a batch of urls, and return dataframe of the resulting data.
    Multi-threaded, cuts download times by 3/4
    url_list: a list of urls of stolen bikes
    """
    
    #
    def find_attribute(att_string,result_table):
        """
        Checks to see if a given attribute is in the table, and returns the value if it is available.
        """
        
        try:
            text = result_table.find(string=re.compile(att_string)).parent.next_sibling 
            return text
        except:
            return 'None'

    #start=time.perf_counter()
    
    def bike_download(url):
            
        #Download the page
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        
        #Specifically take the theft reporting information
        results = soup.find("ul", {"class":"attr-list separate-lines"})
        
        #Initialize the catch dictionary, and capture the bike ID
        catch={'Bike ID': url.split('/bikes/')[1]}
        
        #Capture all other info, if present
        for attr in ['Location', 'Locking description', 'Locking circumvented', 'Date stolen', 'Police report']:
            catch[attr]=find_attribute(attr, results)
        return catch
                
    with cf.ThreadPoolExecutor() as executor:
        results=[executor.submit(bike_download, url) for url in url_list]


    catch=[f.result() for f in results]
    
    download_df=pd.DataFrame(catch)
    
    #finish=time.perf_counter()
    
    #print(f'Metrics finished in {round(finish-start,5)} seconds')
    
    return download_df


        

#%%%



#url='https://bikeindex.org/bikes?page=2&stolenness=all'




def search_page_downloader(page_url):
    """
    Download all the bikes per search page that have a US location listed
    Does not download non-us or non location specified thefts
    """
    
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
            clean_urls.append(result.find('a').get('href'))
    
    
    #Download the batch of urls
    df_page=download_batch_url(clean_urls)
    
    return df_page

#df_page=search_page_downloader(url)





#%%%


#%%

url_list=['https://bikeindex.org/bikes?page='+str(i)+'&stolenness=all' for i in range(2,10)]


overall_start=time.perf_counter()

with cf.ThreadPoolExecutor() as executor:
    results=[executor.submit(search_page_downloader, url) for url in url_list]


catch=[f.result() for f in results]
df_downloaded=pd.concat(catch)

overall_finish=time.perf_counter()

print(f'Overall finished in {round(overall_finish-overall_start,5)} seconds')

#%%

#df_downloaded['Police report'] = df_downloaded['Police report'] != 'None'
#df_downloaded['Police report'] = df_downloaded['Police report'].replace({True:1, False:0})

#%%

def get_final_page():
    url='https://bikeindex.org/bikes?stolenness=stolen'
    
    #Download the page
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    #Find the last page of the stolen bikes
    results = soup.find("div", {"class":"pagination"})
    last_page=results.find_all('a')[-1].get('href')
    last_page=last_page.split('page=')[1].split('&stolen')[0]
    
    return int(last_page)

get_final_page()
#%%

max_page=20
interval=10

def full_download(interval=10, max_page=20, min_page=2):
    problem_sections=[] 
    
    #Start the overall clock 
    overall_start=time.perf_counter()
    
    i=min_page
    while i < max_page:
        
        try:
            #Generate all URLs for this section 
            if i+interval > max_page:
                i_end=max_page+1
            else:
                i_end=i+interval
                
            url_list=['https://bikeindex.org/bikes?page='+str(i)+'&stolenness=all' for i in range(i,i_end)]
                
            #Start clock for this section    
            section_start=time.perf_counter()
            
            #Thread the page downloads
            with cf.ThreadPoolExecutor() as executor:
                results=[executor.submit(search_page_downloader, url) for url in url_list]
            
            #Concatinate the chunk's results
            catch=[f.result() for f in results]
            df_downloaded=pd.concat(catch)
            
            #For privacy, change the report number to a binary value
            df_downloaded['Police report'] = df_downloaded['Police report'] != 'None'
            df_downloaded['Police report'] = df_downloaded['Police report'].replace({True:'Reported', False:'None'})
            
            #Save the file
            df_downloaded.to_csv(f'./Data/Page {i}-{i_end}.csv')
            
            
            section_finish=time.perf_counter()
            
            print(f'                    -----------                \n')
            print(f'Section {i}-{i_end} finished in {round(section_finish-section_start,5)} seconds')    
            print(f'\n                    -----------                \n')
            
        except:
            problem_sections.append(f'{i} - {i_end}')
            print(f'\nERROR: {i} - {i_end}\n')
        
        i+=interval
    
    #Overall time    
    overall_finish=time.perf_counter()
    print(f'Overall finished in {round(overall_finish-overall_start,5)} seconds')   
    
    return problem_sections
    
#%%
    
full_download(interval=10, max_page=get_final_page(),  min_page=4762)

#%%




















































































































#%%












































