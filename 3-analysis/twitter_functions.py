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
import bz2




# Notebook 1 - get timeline by users

def get_tweets(index_block,n_blocks):
    path_to_timeline='../data/timelines/API/IDF-old/'
    bzs = sorted([x for x in os.listdir(path_to_timeline) if x[-4:]=='.bz2'])
    bzs=bzs[:16]
    df=pd.DataFrame()
    for bz in bzs[n_blocks*index_block:n_blocks*(index_block+1)]:
        print(str(bz))
        with bz2.open(path_to_timeline+bz,'rb') as f:

            for line in f:
                tweets_user = json.loads(line.decode("utf-8"))
                tweets_user = pd.DataFrame(tweets_user, columns=['id_str','user','full_text','created_at','lang'])
                df = pd.concat([df,tweets_user])
    return df

                    
    