# Tealium Event ETL
# R. Wagner, 2022

import db3 # wrapper functions for boto3 interactions
import boto3
import pandas as pd   
pd.options.mode.chained_assignment = None  # default='warn'
import json
from datetime import datetime, timezone
import gzip
import re

class tealiumETL:
    def __init__(self, config): 
        for key, value in config.items(): # loop through config dictionary, initialize member variables
            setattr(self, key, value)

        self.tealium_s3_client = self.__tealiumS3Client()
        self.tealium_s3_resource = self.__tealiumS3Resource()

    def __tealiumS3Client(self):
        return boto3.client(service_name='s3',
                            region_name=self.tealium_aws_region, 
                            aws_access_key_id=self.tealium_access_key_id, # fetched from AWS Secrets Manager
                            aws_secret_access_key=self.tealium_secret_access_key)

    def __tealiumS3Resource(self):
        return boto3.resource(service_name='s3',
                              region_name=self.tealium_aws_region, 
                              aws_access_key_id=self.tealium_access_key_id, # fetched from AWS Secrets Manager
                              aws_secret_access_key=self.tealium_secret_access_key)

    def get_last_object(self):
        '''
        Queries the Redshift table that stores the list of loaded event feed objects, 
        returns the name of the object that was last successfully loaded.
        '''
        db3.log(type='info', message='Retrieving name of last loaded event feed object.')
        get_last_object_query = f'''
                                 select object_key 
                                 from {self.object_list_schema}.{self.object_list_table}
                                 where last_modified_utc = (select max(last_modified_utc) from {self.object_list_schema}.{self.object_list_table})
                                 '''
        get_last_object_response = db3.execute_statement(query=get_last_object_query) # execute query
        db3.validate_query(response_id=get_last_object_response['Id']) # wait until query is finished
        db3.log(type='info', message='Retrieving statement results.')
        get_last_object_result = db3.get_statement_result(response=get_last_object_response) # get result contents of query
        last_object_key = get_last_object_result['Records'][0][0]['stringValue'] # extract key of last loaded object
        
        db3.log(type='info', message=f'Last loaded object: {last_object_key}')
        return last_object_key

    def list_unloaded_objects(self, last_object_loaded):
        '''
        Returns list of any event feed objects in Tealium's S3 bucket that were created after 
        the object that was last successfully imported into Redshift cluster.
        '''
        db3.log(type='info', message='Checking Tealium S3 for unloaded objects.')
        try:
            response = self.tealium_s3_client.list_objects_v2(Bucket=self.tealium_bucket_name,
                                                              Prefix=self.tealium_prefix,
                                                              StartAfter=last_object_loaded)
        except Exception as e:
            db3.log(type='error', message='Error checking Tealium S3 for unloaded objects.', e=e)
        else:
            if response.get('Contents') == None:
                db3.log(type='warn', message='No files to load.')
                return None
            else:
                object_list = pd.json_normalize(response['Contents'])
                object_list = object_list[['Key', 'LastModified']]
                object_list.rename(columns = {"Key": "object_key", "LastModified":"last_modified"}, inplace=True)
                db3.log(type='info', message=f'{len(object_list.index)} file(s) to extract.')
                return object_list
    
    def extract_objects(self, object_list):
        '''
        Extract any unloaded objects from the list returned by list_unloaded_objects(), lightly clean for loading into Redshift cluster.
        '''
        # add empty column to object list to store that object's colnames
        db3.log(type='info', message='Extracting unloaded objects from Tealium S3 bucket.')
        object_list['colnames'] = None

        for index, row in object_list.iterrows():
            object_key = row['object_key']
            object_key_destination_name = re.sub(self.tealium_prefix, '', object_key) # strip prefix from filename; contains '/' and is treated as filepath
            
            # extract objects
            db3.log(type='info', message=f'Extracting file {index+1} of {len(object_list.index)}: {object_key}')
            try:
                self.tealium_s3_resource.Bucket(self.tealium_bucket_name).download_file(object_key, object_key_destination_name)
            except Exception as e:
                db3.log(type='error', message=f'Error extracting file {index+1} of {len(object_list.index)}: {object_key}', e=e)
            
            # clean objects
            db3.log(type='info', message=f'Cleaning file {index+1} of {len(object_list.index)}.')
            try:
                # conversion process here is bytes > strings > dicts > data frame
                # for strings, it's one list where each element is a stringified json dict
                with gzip.open(object_key_destination_name, 'rb') as f:
                    bytes = f.read()
        
                # convert bytes to strings (creates a list of strings)
                strings = bytes.decode('utf8').split('\n')
                strings = json.loads(json.dumps(strings)) 

                # convert each string to dict, append to new list
                dicts = []
                for row in strings: 
                    dicts.append(json.loads(row))
        
                # convert to dataframe, subset, rename
                df = pd.DataFrame(dicts) # convert list of dicts to dataframe 
                df_clean = df[df.columns.intersection(self.keep_cols)] # subset to only cols that are in keep list
                df_clean.rename(columns = self.rename_dict, inplace=True)
        
                # per Tealium docs, 'eventtime' is stored as UNIX/epoch timestamp (but is also x1000 for some reason...)
                df_clean['event_time'] = df_clean['event_time'].apply(lambda x: datetime.fromtimestamp(x/1000).astimezone(tz=timezone.utc))

                # store column names as tuple for COPY command.
                colnames = tuple(df_clean.columns.values.tolist())
                colnames = str(colnames).replace('\'', '') # strip single quotes around names
                object_list.iloc[index, 2] = colnames # add colnames tuple to object list
            except Exception as e:
                db3.log(type='error', message=f'Error cleaning file {index+1} of {len(object_list.index)}: {object_key}', e=e)         

            # write file to S3.
            db3.log(type='info', message=f'Loading to S3 file {index+1} of {len(object_list.index)}.')
            try:
                filename = self.bucket_prefix + re.sub('(.gz$)', '.csv', object_key_destination_name) # swap in correct extension 
                db3.s3_resource.Object(self.bucket_name, filename).put(Body=df_clean.to_csv(index=False, header=True))
            except Exception as e:
                db3.log(type='error', message=f'Error loading to S3 file {index+1} of {len(object_list.index)}: {object_key}', e=e) # suppress exception chaining
    
    def load_objects(self, object_list, split_temp_tables=False):
        '''
        Copies objects from S3 bucket into the target table in Redshift cluster.
        '''
        # for QA: option to create indexed temp tables
        if split_temp_tables:
            i=1
        else:
            i = ''

        # loop through each item in loading dictionary (key = object key, value = column names) and upsert into Redshift
        for index, row in object_list.iterrows():
            db3.log(type='info', message=f"Begin upsert process for file {index+1} of {len(object_list.index)}: {row['object_key']}")
            raw_object_key = row['object_key'] # storing key with prefix for object list
            object_key = re.sub(self.tealium_prefix, '', row['object_key']) # strip prefix from filename
            object_key = re.sub('(.gz$)', '.csv', object_key) # swap in CSV extension
            
            last_modified = row['last_modified']
            colnames = row['colnames']

            # create temp table
            create_temp_table_query = f'''
                                       create table if not exists {self.target_schema}.{self.target_table}_temp{i}
                                       (like {self.target_schema}.{self.target_table})
                                       '''
            create_temp_table_response = db3.execute_statement(query=create_temp_table_query)
            db3.validate_query(response_id=create_temp_table_response['Id'])

            # copy contents to temp table
            load_query = f'''
                        copy {self.target_schema}.{self.target_table}_temp{i} {colnames}
                        from 's3://{self.bucket_name}/{self.bucket_prefix}{object_key}'
                        credentials '{self.iam_role}'
                        ignoreheader 1
                        csv
                        '''
            load_response = db3.execute_statement(query=load_query)
            db3.validate_query(response_id=load_response['Id'])

            # delete any duped records in target table
            delete_dupes_query = f'''
                                  delete from {self.target_schema}.{self.target_table}
                                  using {self.target_schema}.{self.target_table}_temp{i}
                                  where {self.target_schema}.{self.target_table}.event_id = {self.target_schema}.{self.target_table}_temp{i}.event_id
                                  '''
            delete_dupes_response = db3.execute_statement(query=delete_dupes_query)
            db3.validate_query(response_id=delete_dupes_response['Id'])

            # insert records into target table. Colnames were included in COPY command and are not needed again here
            insert_query = f'''
                            insert into {self.target_schema}.{self.target_table}
                            (select * from {self.target_schema}.{self.target_table}_temp{i})
                            '''
            insert_response = db3.execute_statement(query=insert_query)
            db3.validate_query(response_id=insert_response['Id'])

            # drop temp table
            drop_temp_table_query = f'drop table {self.target_schema}.{self.target_table}_temp{i}'
            drop_temp_table_response = db3.execute_statement(query=drop_temp_table_query)
            db3.validate_query(response_id=drop_temp_table_response['Id'])

            # insert object key into 'already loaded' list
            log_object_key_query = f'''
                                    insert into {self.object_list_schema}.{self.object_list_table}
                                    values (\'{raw_object_key}\', \'{last_modified}\')
                                    '''
            log_object_key_response = db3.execute_statement(query=log_object_key_query)
            db3.validate_query(response_id=log_object_key_response['Id'])

            db3.log(type='info', message=f"Completed upsert process for file {index+1} of {len(object_list.index)}: {row['object_key']}")

            if split_temp_tables: # if splitting, increment index
                i+=1
