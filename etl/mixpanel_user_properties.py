# Mixpanel: Update User Properties
# R. Wagner, 2022

import os
import db3 # wrapper functions for boto3 interactions
import gzip
import json
import pandas as pd
pd.options.mode.chained_assignment = None
import requests
import math
import time
import random
from io import BytesIO, TextIOWrapper

config = {'iam_role': 'arn:aws:iam::0123456789:role/RedshiftS3',
          's3_bucket': 'glue-assets',
          's3_prefix': 'mixpanel/',
          's3_identifier': 'mixpanel_user_properties',
          'source_schema': 'mixpanel',
          'source_table': 'user_properties',
          'mixpanel_token': '0123456789',
          'batch_size': 2000, # max number of user profiles that can be updated in each request
          'update_all': False, # if true, update all records; useful when adding new properties
          'debug': False,
          'rename_mappings': {'user_id': 'Internal User ID',
                              'name': '$name', # default property; prefix with $
                              'email': '$email', # default property; prefix with $
                              'created_at_utc': 'Account Created at (UTC)', 
                              'is_customer': 'Is Customer',
                              'n_purchases': 'Number of Purchases',
                              'last_purchase_utc': 'Last Purchase (UTC)',
                              'total_revenue': 'Total Revenue',
                              'clv': 'CLV',
                              'cac': 'Customer Acquisition Cost',                              
                              'persona': 'Marketing Persona',                             
                              'updated_at_utc': 'Updated at (UTC)'         
                            }
         }

# unload most recent snapshot from Redshift into S3 (file is saved as 's3://bucket/prefix/identifier_snapshot000.gz')
db3.log(type='info', message='Begin loading most recent snapshot from Redshift to S3...')
unload_snapshot_query = f'''
                          unload ('select * from {config['source_schema']}.{config['source_table']}') 
                          to 's3://{config['s3_bucket']}/{config['s3_prefix']}{config['s3_identifier']}_snapshot'
                          iam_role '{config['iam_role']}' 
                          parallel off
                          delimitere '|' 
                          allowoverwrite
                          addquotes
                          header
                          gzip;
                          '''
unload_temptable_response = db3.execute_statement(query=unload_snapshot_query)
db3.validate_query(response_id=unload_temptable_response['Id'])
db3.log(type='info', message='Completed loading snapshot to S3.')

# import reference/snapshot files into environment
# reference file:
db3.log(type='info', message='Importing reference file...')
try:
    key = f"{config['s3_prefix']}{config['s3_identifier']}_reference.gz"
    filename = f"{config['s3_identifier']}_reference.gz"
    db3.s3_resource.Bucket(config['s3_bucket']).download_file(Key=key, Filename=filename)
    df_reference = pd.read_csv(gzip.open(filename), sep="|")
except Exception as e:
    db3.log(type='error', message='Error importing reference file from S3.', do_raise=True, e=e)
else:
    db3.log(type='info', message='Completed importing reference file.')

# snapshot file:
db3.log(type='info', message='Importing snapshot file...')
try:
    snapshot_key = f"{config['s3_prefix']}{config['s3_identifier']}_snapshot000.gz"
    snapshot_filename = f"{config['s3_identifier']}_snapshot000.gz"
    db3.s3_resource.Bucket(config['s3_bucket']).download_file(Key=snapshot_key, Filename=snapshot_filename)
    df_snapshot = pd.read_csv(gzip.open(snapshot_filename), sep="|")
except Exception as e:
    db3.log(type='error', message='Error importing snapshot file from S3.', do_raise=True, e=e)
else:
    db3.log(type='info', message='Completed importing snapshot file.')

# update records (all vs. select)
if config['update_all']:
    db3.log(type='info', message='Updating all records.')
    upsert_ids = df_snapshot['user_id'].tolist() # all user IDs in snapshot
else:
    # compare MD5 key values in snapshot vs. reference files, update profile if key value differs (=updated record) or no key in reference file (=new record)
    db3.log(type='info', message='Comparing MD5 key values in snapshot/reference files...')
    snapshot_ids = df_snapshot[['user_id', 'key']].rename(columns = {'key':'snapshot_key'})
    reference_ids = df_reference[['user_id', 'key']].rename(columns = {'key':'reference_key'})
    df_merge = snapshot_ids.merge(reference_ids, how='left', on='user_id') # merge old and new keys for comparison 
    upsert_ids = df_merge.loc[df_merge['snapshot_key'] != df_merge['reference_key'], 'user_id'].tolist() # IDs of new/updated records

    # if no profiles have changed, halt program
    if len(upsert_ids) == 0:
        db3.log(type='warn', message='No new/updated records detected. Exiting program.')
        os._exit(0)
    else:
        db3.log(type='info', message=f'{len(upsert_ids)} record(s) to upsert. user IDs: {upsert_ids}')

df_upsert = df_snapshot.loc[df_snapshot['user_id'].isin(upsert_ids)] # grab full records for new/updated users

# create updated reference table for S3 BEFORE df_upsert is further transformed for API calls
# BUT do not load updated table back to S3 until AFTER API call(s) successfully completed (in case the job fails)
df_s3 = df_reference.loc[~df_reference['user_id'].isin(upsert_ids)] # drop stale records from reference table
df_s3 = pd.concat([df_s3, df_upsert], axis=0, ignore_index=True) # append fresh records in their place

# prepare df_upsert for API call.
df_upsert = df_upsert.replace({'t':'true', 'f':'false'}) 
df_upsert['$token'] = config['mixpanel_token']  # project token must be included in EACH payload object (i.e., each row in df)
df_upsert['$distinct_id'] = df_upsert['user_id'] # add copy of user ID: (1) to be used for below group_by (and then dropped from property set), and (2) as an actual user property
df_upsert["$ip"] = "0" # this tells Mixpanel to ignore the IP from this job's network requests, and preserve the user's existing location
df_upsert.rename(columns = config['rename_mappings'], inplace=True) # rename columns per config

# for QA, save new/old snapshot locally (for manual comparison) and halt before files loaded to Mixpanel/S3
if config['debug']:
    df_s3.to_csv('df_snapshot.csv', index=False)
    df_reference.to_csv('df_reference.csv', index=False)
    db3.log(type='warn', message='DEBUG MODE. Saving existing/updated reference files locally; exiting program before loading files to Mixpanel/S3.')
    os._exit(0)

# batch & send records to Mixpanel
num_batches = math.ceil(len(df_upsert) / config['batch_size'])
db3.log(type='info', message=f'Begin posting records to Mixpanel (will post in {num_batches} batch(es)).')

# for each batch, format as JSON and send
for i in list(range(0,num_batches)):
    lower_bound = int(i * config['batch_size'])
    if len(df_upsert.index) > int(lower_bound + config['batch_size']): # upper bound limited by number of records
        upper_bound = int(lower_bound + config['batch_size'])
    else:
        upper_bound = len(df_upsert.index)
    db3.log(type='info', message=f'Begin batch {i + 1} of {num_batches}: [{lower_bound}:{upper_bound-1}]') 

    try:
        # convert each row to a JSON object: {$token, $distinct_id, $ip, $set{<properties>}}
        df_i = df_upsert[lower_bound:upper_bound]
        df_grouped = (df_i.groupby(['$token','$distinct_id', '$ip'])
                        .apply(lambda x: x.drop(['$token','$distinct_id','$ip', 'key'], axis=1).to_dict('records')[0])
                        .reset_index()
                        .rename(columns={0:'$set'})
                        .to_json(orient='records'))
        payload = json.loads(df_grouped)

        # attempt send, pause and retry if rate limit exceeded or error
        request_completed=False
        tries = 0
        while not request_completed:
            if (tries) > 5:
                db3.log(type='error', message='Too many failed attempts. Exiting program.', do_raise=True, e=e)

            db3.log(type='info', message=f'Sending request... (attempt {tries+1})')
            response = requests.post(url="https://api.mixpanel.com/engage?verbose=1#profile-batch-update",
                                     headers={
                                        "Accept": "text/plain",
                                        "Content-Type": "application/json"
                                     },
                                     json=payload
                                    )
            if response.status_code == 429 or response.status_code >= 500:
                db3.log(type='warn', message=f'Status code {response.status_code}. Will wait {min(2 ** tries, 60)} seconds and try again.')
                time.sleep(min(2 ** tries, 60) + random.randint(1, 5)) # if timeout, wait minimum of (2^tries) or 60 seconds before retry.
                tries += 1
                continue
            elif response.status_code == 200:
                db3.log(type='info', message=f'Completed batch {i + 1} of {num_batches}.')
                request_completed=True
    except Exception as e:
        db3.log(type='error', message='Error sending batch.', do_raise=True, e=e)

# send updated reference file back to S3 
db3.log(type='info', message='Overwriting updated reference table to S3...')

try:
    gz_buffer = BytesIO()
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        df_s3.to_csv(TextIOWrapper(gz_file, 'utf8'), sep='|', index=False)
        db3.s3_resource.Object(config['s3_bucket'], key=f"{config['s3_prefix']}{config['s3_identifier']}_reference.gz").put(Body=gz_buffer.getvalue())
except Exception as e:
        db3.log(type='error', message='Error writing updated reference file to S3.', do_raise=True, e=e)
else:
    db3.log(type='info', message='Completed writing updated reference file to S3.')

print("Job complete.")