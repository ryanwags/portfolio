# DBT Monitoring
# R. Wagner, 2022

import db3 # wrapper functions for boto3 interactions
import requests
import json
import pandas as pd
import re
from datetime import datetime
from pytz import timezone

class dbtAudits:
    def __init__(self, config):
        '''
        Loops through config dict in Glue script and initializes each element as a member variable.
        '''
        for key, value in config.items():
            setattr(self, key, value)

    def call_cloud_api(self, limit=None, job_id=None, run_id=None):
        '''
        Wrapper function to call the DBT Cloud API's "Runs" endpoint, which returns a list of runs and some run-level metadata.
        In the future, this could be generalized for the API's three other endpoints, but there are no current use cases for them.
        
        Parameters:
            limit (int):
                The number of records to return. Per the 'order_by' parameter, results are automatically ordered by the 'created at' field before the 'limit' filter is applied.
            job_id (int, optional): 
                The numeric code for a specific job. If provided, will only fetch metadata for runs from that job.
            run_id (int, optional):
                The numeric code for a specific job run. If provided, will only fetch run metadata for that run.

        Returns:
            response: a requests response object (only when the API call is successful)

        '''
        url = f"https://cloud.getdbt.com/api/v2/accounts/{self.dbt_account_id}/runs"
        if run_id: # if manual run ID provided, modify URL
            url = url + f'/{run_id}'
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Token {self.dbt_api_key}"
        }
        # if listing specific run, the only param needed is include_related; however, the others do not need to be removed.
        params = {
            'order_by': '-created_at',     # order by run date (desc)
            'limit': limit,                # fetch last 10 runs (in case last run was manual)
            'include_related': "trigger"   # include trigger fields; can be used to filter out manual runs
        }

        # if custom job ID provided, add to params; will only return runs from that job
        if job_id:
            params['job_definition_id'] = job_id

        db3.log(type='info', message='Calling DBT Cloud API...')
        try:
            response = requests.get(url=url, headers=headers, params=params)
        except Exception as e:
            raise Exception(f'[ERROR] {e}') from None # suppress exception chaining
        else:
            # can return failed status without throwing runtime error; only return response if status=200
            if response.status_code == 200:
                db3.log(type='info', message='DBT Cloud API call successful.')
                return response
            else:
                response_text_json = json.loads(response.text)
                user_message = response_text_json['status']['user_message']
                raise Exception(f'[ERROR] Status {response.status_code}: {user_message}')
   
    def load_run_details(self, target_table, run_id=None):
        '''
        Fetch model-level metadata for a specific run, processes it, and loads it into the target table in Redshift.
        By default, fetches results from most recent scheduled production run. Option to return results from any specific run.

        Parameters:
            target_table (str): 
                The name of Redshift table that the audit results are loaded into. Note that this also serves as the identifier for 
                the .csv file that is written to S3 before being loaded into Redshift.
            run_id (int, optional): 
                The numeric code for a specific job run. If provided, will return the full run results for that run. 
                If NOT provided, will return full run results for most recent scheduled production run.
        '''
        db3.log(type='info', message='Begin DBT Run Details ETL.')
        
        # optional: if given manual run ID, that overrides run list
        if run_id:
            use_id = run_id
            db3.log(type='info', message=f'Manual run ID provided ({use_id}).')
            run = self.fetch_run_list(limit=1, run_id=use_id)
        # otherwise, fetch run list, extract ID of most recent scheduled production run, and filter to that record
        else:
            db3.log(type='info', message='No run ID provided. Fetching ID of most recent scheduled production run.')
            run_list = self.fetch_run_list(limit=10, job_id=self.dbt_production_job_id, scheduled_only=True)
            use_id = run_list.loc[run_list['should_start_at'] == run_list['should_start_at'].max(), 'run_id'].iloc[0] # find most recent Run ID
            db3.log(type='info', message=f'Using run ID {use_id}.')
            run = run_list[run_list['run_id'] == use_id] # filter to that record

        # extract job ID to feed into metadata API call (for manual or automatic run IDs) 
        use_job_id = run['job_id'][0] 

        # extract some run metadata to append to finished table before loading to Redshift
        run = run[['run_id', 'href', 'started_at']]
        run.rename(columns = {'started_at': 'run_started_at'}, inplace=True) # to distinguish from MODEL start times in finished table

        # construct query for metadata API
        query = f"""{{
            models(jobId: {use_job_id}, runId: {use_id}) {{
                runId
                jobId
                uniqueId
                name
                description
                schema
                error
                status
                skip
                compileStartedAt
                compileCompletedAt
                executeStartedAt
                executeCompletedAt
                executionTime
                runGeneratedAt
                runElapsedTime
            }}
            }}"""

        # fetch results
        response = self.call_metadata_api(query=query)

        # process results
        db3.log(type='info', message='Processing Metadata API response object.') 
        try:
            json_data = json.loads(response.text)
            df = json_data['data']['models']
            df = pd.DataFrame(df)

            # rename columns
            df.rename(columns = {'runId': 'run_id',
                                'jobId': 'job_id',
                                'uniqueId': 'unique_id',
                                'name': 'name',
                                'description': 'description',
                                'schema': 'schema',
                                'error': 'error',
                                'status': 'status',
                                'skip': 'skip',
                                'compileStartedAt': 'compile_started_at',
                                'compileCompletedAt': 'compile_completed_at',
                                'executeStartedAt': 'execute_started_at',
                                'executeCompletedAt': 'execute_completed_at',
                                'executionTime': 'execution_time',
                                'runGeneratedAt': 'run_generated_at',
                                'runElapsedTime': 'run_elapsed_time'                    
                                }, 
                    inplace=True)

            # merge run metadata (href and started_at timestamp)
            df = df.merge(run, on='run_id')

            # format timestamps, create PT versions, strip NaT (causes issues in Tableau)
            cols_utc = ['compile_started_at', 'compile_completed_at', 'execute_started_at', 'execute_completed_at', 'run_started_at']
            cols_pt = ([col + '_pt' for col in cols_utc])
            df[cols_utc] = df[cols_utc].apply(lambda x: pd.to_datetime(x))
            df[cols_pt] = df[cols_utc].apply(lambda x: x.dt.tz_convert("US/Pacific"))
            df[cols_utc + cols_pt] = df[cols_utc + cols_pt].apply(lambda x: x.replace({'NaT': None}) )

            # final re-order
            df = df[['run_id','job_id','unique_id','name','description','schema','error','status','skip','compile_started_at',
                    'compile_started_at_pt','compile_completed_at','compile_completed_at_pt','execute_started_at',
                    'execute_started_at_pt','execute_completed_at','execute_completed_at_pt','execution_time',
                    'run_started_at','run_started_at_pt','run_elapsed_time','href']]

            # add 'updated at' timestamp (in PT)
            df['updated_at_pt'] = datetime.now(timezone('US/Pacific'))

        except Exception as e:
            raise Exception(f'[ERROR] {e}') from None # suppress exception chaining
        else:
            # write to S3
            db3.write_s3(df=df, bucket_name=self.bucket_name, prefix=self.bucket_prefix, filename=target_table)

            # insert records into empty table. Executing all queries as one transaction to ensure completion. 
            query = f'''
                    begin transaction;
                    delete from {self.target_schema}.{target_table}; 
                    copy {self.target_schema}.{target_table}
                    from 's3://{self.bucket_name}/{self.bucket_prefix}{target_table}.csv'
                    credentials '{self.iam_role}'
                    ignoreheader 1
                    csv;
                    end transaction;
                    '''
            response = db3.execute_statement(query=query)
            db3.validate_query(response_id=response['Id'])

            db3.log(type='info', message='Completed DBT Run Details ETL.')
    
    def load_tests(self, target_table, run_id=None):
        '''
        Fetch test results, processes them, and loads finished data into the target table in Redshift.
        By default, fetches results from most recent scheduled run of Test job. Option to return results from any specific run.

        Parameters:
            target_table (str): 
                The name of Redshift table that the audit results are loaded into. Note that this also serves as the identifier for 
                the .csv file that is written to S3 before being loaded into Redshift.
            run_id (int, optional): 
                The numeric code for a specific job run. If provided, will return the full run results for that run. 
                If NOT provided, will return full run results for most recent scheduled production run.
        '''        
        db3.log(type='info', message='Begin DBT Tests ETL.')
        
        # optional: if given manual run ID, that overrides run list
        if run_id:
            use_id = run_id
            db3.log(type='info', message=f'Manual run ID provided ({use_id}).')
            run = self.fetch_run_list(limit=1, run_id=use_id)
        # otherwise, fetch run list, extract ID of most recent scheduled production run, and filter to that record
        else:
            run = self.get_most_recent_run(job_id=self.dbt_test_job_id)
            use_id = run['run_id'][0]

        # extract some run metadata to append to finished table before loading to Redshift
        run = run[['run_id', 'href', 'started_at']]
        run.rename(columns = {'started_at': 'run_started_at'}, inplace=True) # to distinguish from TEST start times in finished table

        # construct query for metadata API
        query = f"""{{
                tests(jobId: {self.dbt_test_job_id}, runId: {use_id}) {{
                    runId
                    jobId
                    name
                    description
                    state
                    columnName
                    status
                    error
                    fail
                    warn
                    skip
                    }}
                    }}"""

        # fetch results
        response = self.call_metadata_api(query=query)

        # process results
        try:
            # flatten as dataframe 
            db3.log(type='info', message='Processing Metadata API response object.')
            json_data = json.loads(response.text)
            df = json_data['data']['tests']
            df = pd.DataFrame(df)

            # rename columns
            df.rename(columns = {'runId': 'run_id',
                                'jobId': 'job_id',
                                'name': 'name',
                                'description': 'description',
                                'state': 'state',
                                'columnName': 'column_name',
                                'status': 'status',
                                'error': 'error',
                                'fail': 'fail',
                                'warn': 'warn',
                                'skip': 'skip'
                                }, 
                    inplace=True)


            # merge run metadata (href and started_at timestamp)
            df = df.merge(run, on='run_id')

            # extract test type into column
            df['test_type'] = df['name'].str.extract('^(relationships_|not_null_|unique_|accepted_values_)', expand=True)

            # extract column name from full 'name' field.
            df['table_name'] = df.apply(lambda x: x['name'].replace(str(x['test_type']),''), axis=1) # first remove test type from string
            df['table_name'] = df.apply(lambda x: re.sub('_'+str(x['column_name'])+'(__.*|\Z)','', x['table_name']), axis=1) # strip everything including/after the column name

            # clean test type values (blank = custom test)
            test_types_raw = ['unique_', 'not_null_', 'accepted_values_', 'relationships_', None]
            test_types_clean = ['Unique', 'Not Null', 'Accepted Values', 'Relationships', 'Custom']
            df['test_type'] = df['test_type'].replace(test_types_raw, test_types_clean)

            # Create PT version of 'started at' timestamp for Tableau reporting
            df['run_started_at_pt'] = pd.to_datetime(df['run_started_at']).dt.tz_convert("US/Pacific")

            # add 'updated at' timestamp (in PT)
            df['updated_at_pt'] = datetime.now(timezone('US/Pacific'))

        except Exception as e:
            raise Exception(f'[ERROR] {e}') from None # suppress exception chaining
        else:
            # write table to S3
            db3.write_s3(df=df, bucket_name=self.bucket_name, prefix=self.bucket_prefix, filename=target_table)

            # insert records into empty table; executing all queries as one transaction to avoid partial completion
            query = f'''
                    BEGIN TRANSACTION;
                    DELETE FROM {self.target_schema}.{target_table}; 
                    COPY {self.target_schema}.{target_table}
                    FROM 's3://{self.bucket_name}/{self.bucket_prefix}{target_table}.csv'
                    CREDENTIALS '{self.iam_role}'
                    IGNOREHEADER 1
                    CSV;
                    END TRANSACTION;
                    '''
            response = db3.execute_statement(query=query)
            db3.validate_query(response_id=response['Id'])

            db3.log(type='info', message='Completed DBT Tests ETL.')
