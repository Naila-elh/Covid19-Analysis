#!/usr/bin/env python
# coding: utf-8

# In[1]:
import os
import multiprocessing as mp
import numpy as np
import pandas as pd
from glob import glob
import json
import tweepy
import uuid



path_to_data='../data'
path_to_users = os.path.join(path_to_data + '/users')
path_to_keys = os.path.join('../keys')
path_to_timelines = os.path.join(path_to_data,'timelines','API')


def get_env_var(varname,default):
    
    if os.environ.get(varname) != None:
        var = int(os.environ.get(varname))
        print(varname,':', var)
    else:
        var = default
        print(varname,':', var,'(Default)')
    return var

# Choose Number of Nodes To Distribute Credentials: e.g. jobarray=0-4, cpu_per_task=20, credentials = 90 (<100)
SLURM_JOB_ID            = get_env_var('SLURM_JOB_ID',0)
SLURM_ARRAY_TASK_ID     = get_env_var('SLURM_ARRAY_TASK_ID',0)
SLURM_ARRAY_TASK_COUNT  = get_env_var('SLURM_ARRAY_TASK_COUNT',1)
SLURM_JOB_CPUS_PER_NODE = get_env_var('SLURM_JOB_CPUS_PER_NODE',mp.cpu_count())


def get_key_files(SLURM_ARRAY_TASK_ID,SLURM_ARRAY_TASK_COUNT,SLURM_JOB_CPUS_PER_NODE):

    # Randomize set of key files using constant seed
    np.random.seed(0)
    all_key_files = np.random.permutation(glob(os.path.join(path_to_keys,'key*')))
    auth_file = np.random.permutation(glob(os.path.join(path_to_keys,'auth*')))
    
    # Split file list by node
    key_files = np.array_split(all_key_files,SLURM_ARRAY_TASK_COUNT)[SLURM_ARRAY_TASK_ID]
    
    # Check that node has more CPU than key file 
    if len(key_files) <= SLURM_JOB_CPUS_PER_NODE:
        print('# Credentials Allocated To Node:', len(key_files)) 
    else:
        print('Check environment variables:')
        print('# Credentials (',len(key_files),') > # CPU (', SLURM_JOB_CPUS_PER_NODE,')')
        print('Only keeping', SLURM_JOB_CPUS_PER_NODE, 'credentials')
        key_files = key_files[:SLURM_JOB_CPUS_PER_NODE]
        
    return key_files, auth_file
key_files, auth_file = get_key_files(SLURM_ARRAY_TASK_ID,SLURM_ARRAY_TASK_COUNT,SLURM_JOB_CPUS_PER_NODE)


def get_auth(key_file):
    
    # Import Auth keys
    for auth_file in glob(os.path.join(path_to_keys,'auth*')) :
        with open (auth_file) as f:
            auth_key = json.load(f)
    
    # Import token pairs
    with open(key_file) as f:
        key = json.load(f)

    # OAuth process, using the keys and tokens
    auth = tweepy.OAuthHandler(auth_key['consumer_key'], auth_key['consumer_secret'])
    auth.set_access_token(key['access_token'], key['access_token_secret'])

    # Creation of the actual interface, using authentication
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    try:
        api.verify_credentials()
        print(key_file,": Authentication checked")
    except:
        print(key_file,": error during authentication")
        sys.exit('Exit')
    
    return auth, api



def get_timeline(user_id,api):
    
    timeline = []
    error = None
    
    # Collect All Statuses in Timeline
    try:
        cursor = tweepy.Cursor(
        api.user_timeline, 
        user_id=user_id, 
        count=3200,
        tweet_mode="extended", 
        include_rts=True).items()
        
        for status in cursor:
            timeline.append(status._json)
     
    except tweepy.error.TweepError as e:
        error = str(e)
        
    return pd.DataFrame(timeline), error


# In[2]:
cutoff=100

def download_timelines(index_key,users_list):

    # Create Access For Block of Users
    api = get_auth(key_files[index_key])[1]
    
    # Select Block of Users
    users_block = np.array_split(users_list,len(key_files))[index_key]
    
    # Initialize Output File ID
    output_id = str(uuid.uuid4())
    
    # Initialize DataFrame
    timelines = pd.DataFrame()
    
    # Initialize Downloaded User List
    downloaded_ids = []
    
    for user_id in users_block:
        # Try Downloading Timeline
        timeline, error = get_timeline(user_id,api)
        
        if error!=None:
#             print(user_id,index_key,error)
            continue
        # Append
        print('append')
        timelines = pd.concat([timelines, timeline],sort=False)
        downloaded_ids.append(user_id)
            
        # Save after <cutoff> timelines or when reaching last user
        if len(downloaded_ids) == cutoff or user_id == users_block[-1]:
            print('save')

            filename = \
            'timelines-'+\
            str(SLURM_JOB_ID)+'-'+\
            str(SLURM_ARRAY_TASK_ID)+'-'+\
            str(index_key)+'-'+\
            str(len(downloaded_ids))+'-'+\
            output_id+'.json.bz2'
            
            print('Process', index_key, 'saving', len(downloaded_ids), 'timelines with output file:', 
            os.path.join(path_to_timelines,'IDF',filename))
            
            # Save as list of dict discarding index
            print('save as list')
            timelines.to_json(
            os.path.join(path_to_timelines,'IDF',filename),
            orient='records',
            #force_ascii=False,
            date_format=None,
            double_precision=15)
            
             # Save User Id and File In Which Its Timeline Was Saved
#             print('save user_id')
#             with open(os.path.join(path_to_timelines,'IDF','success'), 'a', encoding='utf-8') as file:
#                 for downloaded_id in downloaded_ids:
#                     file.write(downloaded_id+'\t'+filename+'\n')
            
            # Reset Output File ID, Data, and Downloaded Users
            print('reset')
            del timelines, downloaded_ids
            output_id = str(uuid.uuid4())
            timelines = pd.DataFrame()
            downloaded_ids = []
    return 0


# In[ ]:




