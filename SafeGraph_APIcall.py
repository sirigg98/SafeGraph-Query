# -*- coding: utf-8 -*-

import json as js
import requests
import pandas as pd
from datetime import datetime as dt
import os
import concurrent.futures
import time
from requests.exceptions import Timeout, RequestException
from concurrent.futures import wait, ALL_COMPLETED
import IPython as ipy

start = dt.now()
# Change working directory
os.chdir('C:\\Users\\F0064WK\\OneDrive - Tuck School of Business at Dartmouth College\\Documents\Trade sentiments\data\Safegraph')

dates = ['2019-04-20', '2019-04-27', '2019-05-04', '2019-06-08', '2019-06-15',
         '2020-04-20', '2020-04-27', '2020-05-04', '2020-06-08', '2020-06-15']

url = "https://api.safegraph.com/v1/graphql"



headers = {
    'apikey': 'JQ7SDQqXb4JMWvEtMaRXj2rYbR1LDPoH',
    'content-type': 'application/json'
}

# for x in dates:
#   patterns_df = []

# for i in range(1077,10000):

######################### SafeGraph Query module ############################


def SafeGraph_query(q, date):
    start = time.perf_counter()
    # Queries (for first query and following queries)
    #q = 1
    k= 500*q
    if q == 0:
        query = '''query {
          search(first: 500 filter: {
            address:{
              iso_country_code: "US"
            }
          }) {
            safegraph_weekly_patterns (date: "''' + date + '''") {
              placekey
              parent_placekey
              location_name
              city
              region
              postal_code
              iso_country_code
              safegraph_brand_ids
              date_range_start
              raw_visit_counts
              raw_visitor_counts
              visits_by_day
              poi_cbg
              visitor_home_cbgs {
                key
                value
              }
              visitor_home_aggregation {
                key
                value
              }
              visitor_country_of_origin {
                key
                value
              }
              distance_from_home
              median_dwell
            }
          }
        }'''
    else:
        query = '''query {
              search(first: 500 after: ''' + str(k) + ''' filter: {
                address:{
                  iso_country_code: "US"
                }
              }) {
                safegraph_weekly_patterns (date: "''' + date + '''") {
                  placekey
                  parent_placekey
                  location_name
                  city
                  region
                  postal_code
                  iso_country_code
                  safegraph_brand_ids
                  date_range_start
                  raw_visit_counts
                  raw_visitor_counts
                  visits_by_day
                  poi_cbg
                  visitor_home_cbgs {
                    key
                    value
                  }
                  visitor_home_aggregation {
                    key
                    value
                  }
                  visitor_country_of_origin {
                    key
                    value
                  }
                  distance_from_home
                  median_dwell
                }
              }
            }'''
# API call
    p = 1
    print(f"Try no. {p} for query {q+1}")
    while p < 2:
        response = requests.post(url, headers=headers, json={'query': query})
        if response.status_code != 200:
            print("An Error occurred for query " + str(q+1) + " on try no." + str(p) + " (Code: " + str(response.status_code) + ")") 
            pass
        else: 
            break
        p += 1
#Exception handling. Try-Exception was not working... figure out why 
    if response.status_code != 200:
        print(f"emptydf for query {q+1}. Response time: {response.elapsed}s")
        emptydf = pd.DataFrame()
        return emptydf, q
    else:  
# as python dict
        r_json = js.loads(response.text)
    
    # as pandas dataframe
        patterns_df = pd.json_normalize(r_json['data']['search'])
        finish = time.perf_counter()
    
        print(f'Query no. {q+1} finished in {round(finish-start, 2)} s in {p} try/tries')
        return patterns_df, q

###############################################################################

for date in dates: 
    print("start querying for " + date)
    
    final_df = pd.DataFrame()
    fail = []
    
    
    with concurrent.futures.ThreadPoolExecutor(max_workers = 5) as executor:
        results = [executor.submit(SafeGraph_query, i, date) for i in range(16000)]
        concurrent.futures.wait(results, return_when=ALL_COMPLETED)
        for f in concurrent.futures.as_completed(results):
            f_df = f.result()[0]
            final_df = f_df.append(final_df)
            if f_df.empty:
                fail.append(f.result()[1]+1)
            # try:
            #     f_df = f.result()
            #     final_df = f_df.append(final_df)
            # except TypeError:
            #     f_df = pd.DataFrame()
            #     final_df = f_df.append(final_df)
            
    finish = dt.now()
    print(f'Started querying for {date} at {start} finished at {finish}')
    # # Save with date as filename to csv
    final_df.to_csv(f"{date}.csv")