#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  8 20:31:48 2019

@author: hantswilliams

https://github.com/googlemaps/google-maps-services-python

This script can be used to take in a set of values, e.g., from a CSV, JSON,
XML, etc...and then upload them to google's API places service, and return
the LAT and LONG coordinates for the zipcode. The LAT / LON can then be used
to calculate distances 

"""

import pandas as pd 
import googlemaps
from datetime import datetime


#Bring in Secret API key from file 
location = '/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/vh_zipcode/scripts/API_key_secret/'
keys_file = open(location + "googlemapapikey.txt")
lines = keys_file.readlines()
key_api_map = lines[0].rstrip()

#Load the Key into the Google API service 
#gmaps = googlemaps.Client(key='INSERT IT HERE')
gmaps = googlemaps.Client(key=key_api_map)


#load data files from kel


path = '/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/vh_zipcode/data_files/input/'
filename1 =  path + 'FL Zip Codes' + '.csv'
intputdf = pd.read_csv(filename1, index_col=False)
#intputdf = intputdf.head(50)


#multiple zip request - example using a couple zipcodes from san francisco 
list1 = intputdf['zipcode']
list2 = []

for i in list1:      
    geocode_result = gmaps.geocode([i])
    """ MAYBE HERE ADD IN IF filter(none,list2) != none, then do something"""
    """this will get around the issue of the single missing value"""
    list2.append(geocode_result)  
    
#remove any potential missing values, no returns    
list2 = list(filter(None, list2))

#create a list of values that were not found in the analysis 
""" insert in here """ 

#pull out the individual findings from each / latitude and longitude 
def pullingoutdata():
    return_zip = [item[0]["address_components"][0]["long_name"] for item in list2]
    return_lat = [item[0]["geometry"]["location"]["lat"] for item in list2]
    return_lon = [item[0]["geometry"]["location"]["lng"] for item in list2]
    df1 = pd.DataFrame({'Zipcode':return_zip})
    df2 = pd.DataFrame({'Latitude':return_lat})
    df3 = pd.DataFrame({'Longitude':return_lon})
    final = df1.merge(df2, how='left', left_index=True, right_index=True)
    final = final.merge(df3, how='left', left_index=True, right_index=True)
    return final


#############################
#############################

final_list = pullingoutdata()


####Create the permutaitons 

import itertools
permutations = final_list
#permutations = permutations.set_index('zipcode')
#permutations = permutations[['latitude', 'longitude']]
permutations = permutations.round(3)
permutations['simple1'] = list(zip(permutations.Latitude, permutations.Longitude, permutations.Zipcode))
permutations['simple2'] = permutations['simple1']
permutations = permutations[['simple1', 'simple2']]
permutations = pd.DataFrame([e for e in itertools.product(permutations.simple1, permutations.simple2)], columns=permutations.columns)
permutations[['lat1', 'long1', 'zip1']] = pd.DataFrame(permutations['simple1'].tolist(), index=permutations.index)
permutations[['lat2', 'long2', 'zip2']] = pd.DataFrame(permutations['simple2'].tolist(), index=permutations.index)
permutations = permutations.drop(columns=['simple1', 'simple2'])



#Create the distances
##https://stackoverflow.com/questions/43577086/pandas-calculate-haversine-distance-within-each-group-of-rows

import geopy.distance

def distancer_actual(row):
    coords_1 = (row['lat1'], row['long1'])
    coords_2 = (row['lat2'], row['long2'])
    return geopy.distance.VincentyDistance(coords_1, coords_2).miles

permutations['distance_actual'] = permutations.apply(distancer_actual, axis=1)
permutations = permutations[permutations['distance_actual'] != 0]


final = permutations


#Bring over rest of data from the original file 
dfa = intputdf; dfa=dfa.rename(columns={"zipcode": "zip1", "city": "city1", "state": "state1"})
dfa['zip1'] = dfa['zip1'].astype(str)
dfb = intputdf; dfb=dfb.rename(columns={"zipcode": "zip2", "city": "city2", "state": "state2"})
dfb['zip2'] = dfb['zip2'].astype(str)
final_1 = final.merge(dfa, how='left', left_on='zip1', right_on='zip1')
final_2 = final_1.merge(dfb, how='left', left_on='zip2', right_on='zip2')



final_out_complex = final_2[['zip1','city1', 'state1', 'lat1', 'long1', 'zip2', 'city2', 'state2', 'lat2', 'long2', 'distance_actual']]
final_out_simple = final_2[['zip1','city1', 'state1','zip2', 'city2', 'state2','distance_actual']]


import csv
final_out_complex.to_csv('/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/vh_zipcode/data_files/output/examp_file_finalout_complex.csv', index=False)
final_out_simple.to_csv('/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/vh_zipcode/data_files/output/examp_file_finalout_simple.csv', index=False)











