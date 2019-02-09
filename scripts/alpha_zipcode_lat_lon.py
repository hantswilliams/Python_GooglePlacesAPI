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



import googlemaps
from datetime import datetime

gmaps = googlemaps.Client(key='AIzaSyAAUpHA4M1imsFxhSA01vAFuZkmDk-Fu5Y')


"""
#single search request 

geocode_result = gmaps.geocode('94002')
return_zip = geocode_result[0]["address_components"][0]["long_name"]
return_lat = geocode_result[0]["geometry"]["location"]["lat"]
return_lon = geocode_result[0]["geometry"]["location"]["lng"]
d = {'zipcode':[return_zip], 'latitude':[return_lat], 'longitude':[return_lon]}
dataframe = pd.DataFrame(data=d)
"""






#multiple zip request - example using a couple zipcodes from san francisco 
list1 = ['94002', '94070', '94016', '94125', '94134', '94143']
list2 = []

for i in list1:      
    geocode_result = gmaps.geocode([i])
    return_zip = geocode_result[0]["address_components"][0]["long_name"]
    return_lat = geocode_result[0]["geometry"]["location"]["lat"]
    return_lon = geocode_result[0]["geometry"]["location"]["lng"]
    d = {'zipcode':[return_zip], 'latitude':[return_lat], 'longitude':[return_lon]}
    dd = pd.DataFrame(data=d)
    list2.append(dd)  
    
list_api_output = pd.concat(list2)
list_api_output['location1'] = 'San Francisco'


#############################

list1 = ['10003', '10021', '10009', '10280', '10014', '10065']
list2 = []

for i in list1:      
    geocode_result = gmaps.geocode([i])
    return_zip = geocode_result[0]["address_components"][0]["long_name"]
    return_lat = geocode_result[0]["geometry"]["location"]["lat"]
    return_lon = geocode_result[0]["geometry"]["location"]["lng"]
    d = {'zipcode':[return_zip], 'latitude':[return_lat], 'longitude':[return_lon]}
    dd = pd.DataFrame(data=d)
    list2.append(dd)  
    
list_api_output_2 = pd.concat(list2)
list_api_output_2['location1'] = 'New York City'



#############################

final_list = pd.concat([list_api_output, list_api_output_2])


####Create the permutaitons 

import itertools
permutations = final_list[final_list['location1'] == 'San Francisco']
#permutations = permutations.set_index('zipcode')
#permutations = permutations[['latitude', 'longitude']]
permutations = permutations.round(3)
permutations['simple1'] = list(zip(permutations.latitude, permutations.longitude, permutations.zipcode))
permutations['simple2'] = permutations['simple1']
permutations = permutations[['simple1', 'simple2']]
permutations = pd.DataFrame([e for e in itertools.product(permutations.simple1, permutations.simple2)], columns=permutations.columns)
permutations[['lat1', 'long1', 'zip1']] = pd.DataFrame(permutations['simple1'].tolist(), index=permutations.index)
permutations[['lat2', 'long2', 'zip2']] = pd.DataFrame(permutations['simple2'].tolist(), index=permutations.index)
permutations = permutations.drop(columns=['simple1', 'simple2'])



#Create the distances
##https://stackoverflow.com/questions/43577086/pandas-calculate-haversine-distance-within-each-group-of-rows

def distancer_actual(row):
    coords_1 = (row['lat1'], row['long1'])
    coords_2 = (row['lat2'], row['long2'])
    return geopy.distance.VincentyDistance(coords_1, coords_2).miles

permutations['distance_actual'] = permutations.apply(distancer_actual, axis=1)
permutations['location'] = 'San Francisco'
permutations = permutations[permutations['distance_actual'] != 0]



final = permutations


import csv, math
final.to_csv('/Users/hantswilliams/Dropbox/Biovirtua/Python_Projects/vh_zipcode/test_output.csv', index=False)








