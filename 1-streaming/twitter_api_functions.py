#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import sys
import re
import gzip
import json
import numpy as np
import time
import multiprocessing as mp
import pandas as pd
import geocoder
import shapely
from tweepy import Stream
from tweepy.streaming import StreamListener
from http.client import IncompleteRead


# Notebook 1-Streaming

class MyListener(StreamListener):
    
    
    def on_data(self, data):
        try:
            with open('../data/twitter-stream-geobox-paris-'+time.strftime("%Y-%m-%d-%H", time.gmtime())+'.json', 'a') as f:
                f.write(data)
                return True
        except BaseException as e:
            print('Error on_data: ', str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True

def stream_tweets(auth):# Connect/reconnect the stream
    twitter_stream = Stream(auth, MyListener())
    #twitter_stream.filter(locations=geobox)
    twitter_stream.filter(language='fr')
        

# Notebook 3 - Get_Tweets

def get_tweets(index_block,n_blocks):
    path_to_gzs = '../data/'
    gzs = sorted([x for x in os.listdir(path_to_gzs) if x[-3:]=='.gz'])

    tweets  = []
    for gz in gzs[n_blocks*index_block:n_blocks*(index_block+1)]:
        print(str(gz))
        with gzip.open(path_to_gzs+gz,'rb') as f:

            for line in f:
                try:
                    tweet = json.loads(line.decode("utf-8"))
                    list_columns=[
                    tweet['id_str'],
                    tweet['user']['id_str'],
                    tweet['user']['name'],
                    tweet['text'],
                    pd.to_datetime(tweet['created_at']), 
                    tweet['lang']]

                    if (tweet['place'] is not None): 
                        list_columns.extend([tweet['place']['country'],
                                             tweet['place']['country_code'],
                                             tweet['place']['place_type'],
                                             tweet['place']['name'], 
                                             tweet['place']['full_name'],
                                             tweet['place']['bounding_box']['type'],
                                             tweet['place']['bounding_box']['coordinates']])
                    if (tweet['coordinates'] is not None) :
                        list_columns.extend(tweet['coordinates']['coordinates'][1], tweet['coordinates']['coordinates'][0])
                    else :
                        list_columns.extend([np.nan, np.nan])

                    tweets.append(list_columns)
                except:
                    print("error "+str(IOError))
                    pass
    return pd.DataFrame(tweets, columns=['_id','user_id','user_name','text','time','language','country','country_code','place_type','city', 'full_name_place','bounding_box_type','bounding_box_coordinates','latitude','longitude'])

