import logging
import polars as pl
import json
import boto3
import pytz
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

#https://stackoverflow.com/questions/46476246/issues-running-airflow-scheduler-as-a-daemon-process

code_run_mode = 'dev' #Options include 'dev' or 'prod'

ptv_config_path = None if code_run_mode == 'prod' else 'ptv_transform_config.json'

with open(ptv_config_path,'r') as f: 
    config_file = json.load(f)

config_var = config_file[code_run_mode]

# -------------------------- VAR DEFINITIONS -------------------------
default_earliest_date_load = config_var['default_earliest_date_load']
parquet_file_path = config_var['parquet_file_path']
full_refresh = bool(config_var['full_refresh'])

# - AWS Configs -
aws_s3_bucket=config_var['aws_configs']['aws_s3_bucket']
aws_s3_prefix=config_var['aws_configs']['aws_s3_prefix']
aws_keys_path = config_var['aws_configs']['aws_keys_path']
aws_default_object = config_var['aws_configs']['default_s3_object']

# - BigQuery Configs -
bigquery_key_path = config_var['bigquery_configs']['bigquery_key_path']
bigquery_project_id = config_var['bigquery_configs']['bigquery_project_id']
bigquery_dataset_id = config_var['bigquery_configs']['bigquery_dataset_id']
bigquery_meta_data_table_id = config_var['bigquery_configs']['bigquery_meta_data_table_id']

date_fields = [         
                'published_on', 
                'last_updated', 
                'from_date', 
                'to_date' 
                ]

disruption_df_schema = ['file_date',
                        'disruption_id', 
                        'title', 
                        'url', 
                        'description', 
                        'disruption_status', 
                        'disruption_type', 
                        'published_on', 
                        'last_updated', 
                        'from_date', 
                        'to_date', 
                        'stops', 
                        'colour', 
                        'display_on_board', 
                        'display_status', 
                        'transport_type']

route_disruption_df_schema = ['file_date',
                              'disruption_id', 
                              'route_type', 
                              'route_id', 
                              'route_name', 
                              'route_number', 
                              'route_gtfs_id', 
                              'direction']

stops_disruption_df_schema = ['file_date',
                                'disruption_id',
                                'stop_id',
                                'stop_name']

cleaned_disruption_df_schema = ['file_date',
                                'disruption_id', 
                                'title', 
                                'url', 
                                'description', 
                                'disruption_status', 
                                'disruption_type', 
                                'published_on', 
                                'last_updated', 
                                'from_date', 
                                'to_date',
                                'published_on_mlb', 
                                'last_updated_mlb', 
                                'from_date_mlb', 
                                'to_date_mlb',   
                                'colour', 
                                'display_on_board', 
                                'display_status', 
                                'transport_type']

cleaned_route_disruption_df_schema = ['file_date',
                                        'disruption_id', 
                                        'route_type', 
                                        'route_id', 
                                        'route_name', 
                                        'route_number', 
                                        'route_gtfs_id']


df_disruptions_info = pl.DataFrame(schema=disruption_df_schema)
df_route_disruptions_info = pl.DataFrame(schema=route_disruption_df_schema)
df_stops_disruption_info = pl.DataFrame(schema=stops_disruption_df_schema)

#------------------------ DEFINE FUNCTIONS----------------------------------
def get_local_time():
    utc_dt = datetime.now(timezone.utc)
    local_tz = "Australia/Melbourne"
    local_time = pytz.timezone(local_tz)
    local_dt = utc_dt.astimezone(local_time).strftime('%Y%m%d%H%M')
    return local_dt

def convert_to_aus_tz(some_dt,tz_format='%Y-%m-%dT%H:%M:%S%z'):
    dt_parsed = datetime.strptime(some_dt,tz_format).strftime('%Y%m%d%H%M')
    return str(dt_parsed)

def get_s3_client():
    with open(aws_keys_path,'r') as f: 
        aws_keys = json.load(f)
    s3_client = boto3.client('s3', 
                             aws_access_key_id=aws_keys['AWS_ACCESS_KEY'], 
                             aws_secret_access_key=aws_keys['AWS_SECRET_KEY']
                             )
    return s3_client


def read_disruption(s3_client=get_s3_client(), Object=aws_default_object): 
    s3_object = s3_client.get_object(Bucket=aws_s3_bucket,Key=Object)
    body_unread = s3_object['Body']
    body = body_unread.read()
    list_of_disruption_types = list(json.loads(body)['disruptions'].keys())
    file_date =Object.replace(aws_s3_prefix,'').replace('.json','')
    all_disruptions = []
    all_route_disruptions = []
    all_stops_disruptions = []
    for disruption_type in list_of_disruption_types:
        disruption_list = json.loads(body)['disruptions'][disruption_type]
        for disruption in disruption_list:
            routes = disruption['routes']
            stops = disruption['stops']
            disruption['transport_type'] = disruption_type
            disruption['file_date'] = file_date
            columns = list(disruption.keys())
            cleaned_disruption = {}
            for col in columns: 
                if col not in ['routes','stops']:
                    cleaned_disruption[col] = disruption[col]
                if col in date_fields: 
                    cleaned_disruption[col+'_mlb'] = convert_to_aus_tz(cleaned_disruption[col]) if cleaned_disruption[col] is not None else None
            for route in routes:
                route_disruption = {}
                route_disruption['disruption_id'] = disruption['disruption_id']
                route_disruption['file_date'] = file_date
                for key in list(route.keys()):
                    route_disruption[key] = route[key]
                all_route_disruptions.append(route_disruption)
            for stop in stops: 
                stops_disruption = {}
                stops_disruption['disruption_id'] = disruption['disruption_id']
                stops_disruption['file_date'] = file_date
                for key in list(stop.keys()):
                    stops_disruption[key] = stop[key]
                all_stops_disruptions.append(stops_disruption)
            all_disruptions.append(cleaned_disruption)  
    return all_disruptions, all_route_disruptions, all_stops_disruptions


def get_file_names_from_bucket(bucket,prefix,big_query_client):
    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    files_to_load =[]
    data_loader_status = {}
    global full_refresh
    latest_date,full_refresh = get_latest_file_loaded(big_query_client)
    print('------------- COMPILING FILES ---------------')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page['Contents']:
            file_name = str(obj['Key'])
            file_date = file_name.replace(prefix,'').replace('.json','')
            if int(file_date) > latest_date:
                print(file_name)
                files_to_load.append(file_name)
                data_loader_status[file_name] = False
                if int(file_date) > int(latest_date):
                    latest_date = int(file_date)
    meta_data = [{'load_date':get_local_time(),'latest_file_loaded':latest_date}]
    return data_loader_status,files_to_load,meta_data

def read_files_from_bucket(data_loader_status,prefix=aws_s3_prefix):
    print('-------------- READING FILES ---------------')
    ptv_disruptions_info = []
    ptv_route_disruptions_info = []
    ptv_stops_disruptions_info = []
    error_log ={}
    for file_name in list(data_loader_status.keys()):
        print(file_name)
        try:
            disruptions_info, route_disruptions_info, stops_disruptions_info= read_disruption(Object=file_name)
            ptv_disruptions_info = ptv_disruptions_info+disruptions_info
            ptv_route_disruptions_info = ptv_route_disruptions_info+route_disruptions_info
            ptv_stops_disruptions_info = ptv_stops_disruptions_info+stops_disruptions_info
            data_loader_status[file_name] = True
        except Exception as e: print(e)
    return data_loader_status,error_log,ptv_disruptions_info, ptv_route_disruptions_info,ptv_stops_disruptions_info

# ---------------- Reading and Loading files to/from BigQuery ------------------ 

def get_big_query_client(bigquery_key_path): 
    credentials = service_account.Credentials.from_service_account_file(bigquery_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    big_query_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    return big_query_client



def check_if_table_exists(big_query_client, table_id):
    try:
        big_query_client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        print("Table {} is not found.".format(table_id))
        return False
    
def get_latest_file_loaded(big_query_client):
    if check_if_table_exists(big_query_client,bigquery_meta_data_table_id):
        sql = """
        SELECT 
        MAX(latest_file_loaded) as latest_file
        FROM `{}` """.format(bigquery_meta_data_table_id)

        df_latest_file_loaded = big_query_client.query(sql).to_dataframe()
        df_latest_file_loaded.head()
        latest_file_loaded = df_latest_file_loaded['latest_file'][0] if df_latest_file_loaded['latest_file'].isnull()[0]==False else default_earliest_date_load 
        return latest_file_loaded,False
    return default_earliest_date_load,True

def write_to_bq(big_query_client,df,table_name,write_disposition="WRITE_APPEND"):
    # Save results as parquet
    # This result in a smaller file size 
    # and will keep intact your schema 
    try:
        table_id = bigquery_project_id+'.'+bigquery_dataset_id+'.'+table_name
        write_disposition = "WRITE_TRUNCATE" if full_refresh else "WRITE_APPEND"
        df.write_parquet(parquet_file_path)
        print("------------ File added as a parquet ----------------")
        # Set configurations for the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition=write_disposition,
        )

        # Open the data a binary 
        with open(parquet_file_path, "rb") as f:
            job = big_query_client.load_table_from_file(f, table_id, job_config=job_config)

        # Execute the job
        job.result()
    except Exception as e:
        # Log the exception if an error occurs
        logging.error(f"Error in transform_data: {str(e)}")
        raise  # Re-raise the exception to mark the task as failed

def transform_data():
    big_query_client = get_big_query_client(bigquery_key_path)
    data_loader_status,files_to_load,meta_data = get_file_names_from_bucket(bucket=aws_s3_bucket,prefix=aws_s3_prefix,big_query_client=big_query_client)
    if len(files_to_load)>0:
        print("--------- More than 0 files to load --------------")
        data_loader_status,error_log,ptv_disruptions_info, ptv_route_disruptions_info,ptv_stops_disruption_info = read_files_from_bucket(data_loader_status)
        
        print("--------- files have been read -----------------")
        
        df_ptv_disruptions = pl.DataFrame(ptv_disruptions_info)
        df_ptv_route_disruptions = pl.DataFrame(ptv_route_disruptions_info)
        df_ptv_stops_disruptions = pl.DataFrame(ptv_stops_disruption_info)
        print("--------- Polars Data frames created --------------")
        
        df_cleaned_ptv_disruptions = df_ptv_disruptions[cleaned_disruption_df_schema]
        df_cleaned_ptv_route_disruptions = df_ptv_route_disruptions[cleaned_route_disruption_df_schema]
        print("--------- Dataframes have had nested columns removed ---------")
        
        df_cleaned_ptv_disruptions.with_columns(date = datetime.now())
        df_cleaned_ptv_route_disruptions.with_columns(date = datetime.now())
        print("-------- Column for the date of load has been added --------")
        
        df_cleaned_ptv_disruptions_unique = df_cleaned_ptv_disruptions.unique()
        df_cleaned_ptv_route_disruptions_unique = df_cleaned_ptv_route_disruptions.unique()
        df_ptv_stops_disruptions_unique = df_ptv_stops_disruptions.unique()
        print("-------- Data has been deduplicated ------------")
        
        meta_data[0]["ptv_disruptions_rows_loaded"] = df_cleaned_ptv_disruptions_unique.shape[0]
        meta_data[0]["ptv_disruptions_routes_rows_loaded"] = df_cleaned_ptv_route_disruptions_unique.shape[0]
        meta_data[0]["ptv_disruptions_stops_rows_loaded"] = df_ptv_stops_disruptions_unique.shape[0]
        print("-------- Meta data updated with row counts ------------")
        
        df_meta_data = pl.DataFrame(meta_data)
        print("-------- meta data has been added to a polars dataframe ---------")
        write_to_bq(big_query_client=big_query_client,df=df_cleaned_ptv_disruptions_unique,table_name="ptv_disruptions")
        print("-------- ptv_disruptions loaded --------------")
        write_to_bq(big_query_client=big_query_client,df=df_cleaned_ptv_route_disruptions_unique,table_name="ptv_disruptions_routes")
        print("-------- ptv_disruptions_routes loaded ---------")
        write_to_bq(big_query_client=big_query_client,df=df_ptv_stops_disruptions_unique, table_name="ptv_disruptions_stops")
        print("------- ptv_disruptions_stops loaded -------------")
        write_to_bq(big_query_client=big_query_client,df=df_meta_data,table_name="ptv_disruptions_meta_data",write_disposition="WRITE_APPEND")
        print("------- ptv_disruptions_meta_data loaded ----------")