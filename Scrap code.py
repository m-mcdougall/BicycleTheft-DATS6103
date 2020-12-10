# -*- coding: utf-8 -*-


"""
Testing methods to concat data quickly
"""

#1st, most basic method
x=pd.DataFrame()
start=time.perf_counter()

catch=[]
for attr in ['Location', 'Locking description', 'Locking circumvented', 'Date stolen', 'Police report']:
    print(f'{attr}: {find_attribute(attr)}')
    catch.append(find_attribute(attr))
    
x=x.append(pd.Series(catch),ignore_index=True)    
finish=time.perf_counter()

print(f'Metrics finished in {round(finish-start,5)} seconds')



#Dictionary is similar in time to list, but is much less likely to have jank values
#Also assigns column names, which is swell.

x=pd.DataFrame()
start=time.perf_counter()

for i in range(1000):
    catch={}
    for attr in ['Location', 'Locking description', 'Locking circumvented', 'Date stolen', 'Police report']:
        #print(f'{attr}: {find_attribute(attr)}')
        catch[attr]=find_attribute(attr)
        
    x=x.append(pd.Series(catch),ignore_index=True)  

finish=time.perf_counter()

print(f'Metrics finished in {round(finish-start,5)} seconds')



#This one is an order of magnitude faster, use this.
start=time.perf_counter()
y=x.copy()
big_catch=[]
for i in range(1000):
    catch={}
    for attr in ['Location', 'Locking description', 'Locking circumvented', 'Date stolen', 'Police report']:
        #print(f'{attr}: {find_attribute(attr)}')
        catch[attr]=find_attribute(attr)
        
    big_catch.append(catch)
        
x=pd.DataFrame(big_catch)

z=pd.concat([y,x])

finish=time.perf_counter()

print(f'Metrics finished in {round(finish-start,5)} seconds')


#%%%

"""
This is the early, non-threaded batch downloader

"""


def download_batch_url(url_list):
    """
    Download a batch of urls, and return dataframe of the resulting data
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

    start=time.perf_counter()
    
    big_catch=[]
    for url in url_list:  
            
        #Download the page
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        
        #Specifically take the theft reporting information
        results = soup.find("ul", {"class":"attr-list separate-lines"})
        
        catch={}
        for attr in ['Location', 'Locking description', 'Locking circumvented', 'Date stolen', 'Police report']:
            catch[attr]=find_attribute(attr, results)
            
        big_catch.append(catch)
                
    download_df=pd.DataFrame(big_catch)
    
    finish=time.perf_counter()
    
    print(f'Metrics finished in {round(finish-start,5)} seconds')
    
    return download_df


#%%
    

"""
The original page downloader
Did not filter out the non-us location specified locations
"""


def search_page_downloader(page_url):
    #Download the page
    page = requests.get(page_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    #Get links of links to bike pages only
    results=soup.find_all('a', attrs={'href': re.compile('^https://bikeindex.org/bikes/')})
    
    #Clean to only links, and set to get unique links only
    results=list(set([result.get('href') for result in results]))
    
    #Download the batch of urls
    df_page=download_batch_url(results)
    
    return df_page

#df_page=search_page_downloader(url)



#%%
    
""""
Long-form code for removing appartment labels, replaced with a function.

"""


for addr_str in test:
    match= pattern.search(addr_str)
    try:
        substring=match[0]
        addr_new=addr_str.replace(substring, '')
        print(addr_str)
        print(addr_new)
        print()
        fixed.append(addr_new)
        
    except:
        to_fix.append(addr_str)
    
test=to_fix
to_fix=[]


print('Apartments with #____ ')
print(f'---------------------\n')

#Removes the apartments that have the prefix #_____
pattern=re.compile("[#][\w|\W]+?[,]")

for addr_str in test:
    match= pattern.search(addr_str)
    try:
        substring=match[0]
        addr_new=addr_str.replace(substring, '')
        print(addr_str)
        print(addr_new)
        print()
        fixed.append(addr_new)
        
    except:
        to_fix.append(addr_str)


test=to_fix
to_fix=[]

#Removes the apartments that have the prefix #_____
pattern=re.compile("[#][\w|\W]+?[,]")

for addr_str in test:
    match= pattern.search(addr_str)
    try:
        substring=match[0]
        addr_new=addr_str.replace(substring, '')
        print(addr_str)
        print(addr_new)
        print()
        fixed.append(addr_new)
        
    except:
        to_fix.append(addr_str)



test=to_fix
to_fix=[]

#Removes the apartments that have just the number between two commas
pattern=re.compile("[,][\W|0-9]+?[,]")

for addr_str in test:
    match= pattern.search(addr_str)

    try:
        substring=match[0]
        addr_new=addr_str.replace(substring, '')
        print(addr_str)
        print(addr_new)
        print()
        fixed.append(addr_new)
        
    except:
        to_fix.append(addr_str)
    
    
test=to_fix




#%%

"""
Original remove apartments, when it returned two lists - the fixed and the remaining problems

"""

def remove_apartments(address_list):
    """
    Removes several common appartment labels that interfere with the location search
    Includes "Apt ___", 'Unit ____,  # ____, and just the number between two commas (eg, ', 201,)
    """
    
    fixed=[]
    
    
    def pattern_loop(pattern, address_list):
        """
        Applies a regex pattern to the list of addresses, and removes
        matching elements from the addresses if found, returning the modified addresses
        and the list of addresses that still need to be fixed
        
        """
    
        internal_fixed=[]
        internal_to_fix=[]
    
        #Search for the regex pattern in each entry
        for addr_str in address_list:
            match= pattern.search(addr_str.title())
            try:
                #If a match was found, eliminate the matched substring
                substring=match[0]
                addr_new=addr_str.title().replace(substring, '')
                internal_fixed.append(addr_new)
                
                print(addr_str)
                print(addr_new)
                print()
    
            except:
                #If match not found, then add to list for further parsing
                internal_to_fix.append(addr_str)
    
        return internal_fixed, internal_to_fix
    
    
    print(f'\nApartments with the word Apartment or Unit')
    print(f'---------------------\n')
    
    #Removes the apartments that have the prefix "Apt" or "Unit"
    pattern=re.compile("\W([Aa]pt|[Uu]nit)[\w|\W]+?[,]")
    out_fixed, out_continue = pattern_loop(pattern, address_list)
    #Append the fixed addresses to the completed fixed list
    fixed+=out_fixed
    
    print(f'\nApartments with the prefix #_____')
    print(f'---------------------\n')
    
    #Removes the apartments that have the prefix #_____
    pattern=re.compile("[#][\w|\W]+?[,]")
    out_fixed, out_continue = pattern_loop(pattern, out_continue)
    fixed+=out_fixed
    
    print(f'\nApartments with format , #### , ')
    print(f'---------------------\n')
    
    #Removes the apartments that have just the number between two commas
    pattern=re.compile("[,][\W|0-9]+?[,]")
    out_fixed, out_continue = pattern_loop(pattern, out_continue)
    fixed+=out_fixed
    
    print(f'\nApartments with the word Ste (studio/suite)')
    print(f'---------------------\n')
    
    #Removes the apartments that have just the number between two commas
    pattern=re.compile("\s[Ss]te[\s,.][\w|\W]+?[,]")
    out_fixed, out_continue = pattern_loop(pattern, out_continue)
    fixed+=out_fixed


    return fixed, out_continue


test_fixed, test_continue = remove_apartments(problems)


#%%

#%%
"""
Check which Geocoder is faster to search

"""

overall_start=time.perf_counter()

from geopy.geocoders import Photon

geolocator = Photon(user_agent="BikeThefts")
location = geolocator.geocode("Alameda & Bannock, Denver, CO 80223")

overall_finish=time.perf_counter()

print(f'Photon finished in {round(overall_finish-overall_start,5)} seconds')




overall_start=time.perf_counter()

from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="BikeThefts")
location = geolocator.geocode("Alameda & Bannock, Denver, CO 80223")

overall_finish=time.perf_counter()

print(f'Nominatim finished in {round(overall_finish-overall_start,5)} seconds')



#Nominatim is faster, but you need to make sure to leave 1 second between queries.


#%%

"""
Check which is faster, Nominatim or Photon,
and which produces more results.
"""

fixed=addresses_fixed[15:30]

overall_start=time.perf_counter()
geolocator = Nominatim(user_agent="BikeThefts")
sucess=0
for i in range(len(fixed)):
        try:
            address=fixed[i]
            location = geolocator.geocode(address)
            
            location.address

        
            #Required interval between requests for Nominatim's TOS
            time.sleep(1)
        except:
            pass
        
print(f'Nominatim success = {sucess}/{len(fixed)}')       
overall_finish=time.perf_counter()

print(f'Nominatim finished in {round(overall_finish-overall_start,5)} seconds\n\n')


overall_start=time.perf_counter()
fixed=addresses_fixed[15:30]

geolocator = Photon(user_agent="BikeThefts")
sucess=0
for i in range(len(fixed)):
        try:
            address=fixed[i]
            location = geolocator.geocode(address)
            
            location.address
            sucess+=1
            
        
        except:
            pass
        
print(f'Photon success = {sucess}/{len(fixed)}')        
overall_finish=time.perf_counter()

print(f'Photon finished in {round(overall_finish-overall_start,5)} seconds\n\n')



#%%

















