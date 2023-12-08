import logging
import pandas as pd
import polars as pl
import requests
import json
import hashlib
import pytz
import boto3
import psycopg2
from hashlib import sha1
from hmac import new
from datetime import datetime,timezone
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

# -------------------------- VAR DEFINITIONS -------------------------
today = datetime.today().strftime('%Y%m%d')
bucket='general-bucket'
prefix='ptv_disruptions_'
aws_keys_path = 'aws_s3_key.json'
months_to_load=2

disruption_df_schema = ['disruption_id', 
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

route_disruption_df_schema = ['disruption_id', 
                              'route_type', 
                              'route_id', 
                              'route_name', 
                              'route_number', 
                              'route_gtfs_id', 
                              'direction']

stops_disruption_df_schema = ['disruption_id',
                                      'stop_id',
                                      'stop_name']

cleaned_disruption_df_schema = ['disruption_id', 
                        'title', 
                        'url', 
                        'description', 
                        'disruption_status', 
                        'disruption_type', 
                        'published_on', 
                        'last_updated', 
                        'from_date', 
                        'to_date',  
                        'colour', 
                        'display_on_board', 
                        'display_status', 
                        'transport_type']

cleaned_route_disruption_df_schema = ['disruption_id', 
                              'route_type', 
                              'route_id', 
                              'route_name', 
                              'route_number', 
                              'route_gtfs_id']


df_disruptions_info = pl.DataFrame(schema=disruption_df_schema)
df_route_disruptions_info = pl.DataFrame(schema=route_disruption_df_schema)
df_stops_disruption_info = pl.DataFrame(schema=stops_disruption_df_schema)

#------------------------ DEFINE FUNCTIONS----------------------------------
def get_s3_client():
    with open(aws_keys_path,'r') as f: 
        aws_keys = json.load(f)
    s3_client = boto3.client('s3', 
                             aws_access_key_id=aws_keys['AWS_ACCESS_KEY'], 
                             aws_secret_access_key=aws_keys['AWS_SECRET_KEY']
                             )
    return s3_client

def two_column_hash(df,col1,col2):
    df_copy = df
    df_copy['combined'] = df_copy[col1].astype(str)+df_copy[col2].astype(str)
    df_copy['hashed'] = df['combined'].apply(hasher)
    return df_copy['hashed']
        
def hasher(x):
    y = x.encode()
    hasher = hashlib.sha256()
    hasher.update(y)
    return hasher.hexdigest()

def read_disruption(s3_client=get_s3_client(), Bucket=bucket,Object='ptv_disruptions_202310031930.json'): 
    s3_object = s3_client.get_object(Bucket=bucket,Key=Object)
    body_unread = s3_object['Body']
    body = body_unread.read()
    list_of_disruption_types = list(json.loads(body)['disruptions'].keys())
    all_disruptions = []
    all_route_disruptions = []
    all_stops_disruptions = []
    for disruption_type in list_of_disruption_types:
        disruption_list = json.loads(body)['disruptions'][disruption_type]
        for disruption in disruption_list:
            routes = disruption['routes']
            stops = disruption['stops']
            disruption['transport_type'] = disruption_type
            columns = list(disruption.keys())
            cleaned_disruption = {}
            for col in columns: 
                if col not in ['routes','stops']:
                    cleaned_disruption[col] = disruption[col]
            for route in routes:
                route_disruption = {}
                route_disruption['disruption_id'] = disruption['disruption_id']
                for key in list(route.keys()):
                    route_disruption[key] = route[key]
                all_route_disruptions.append(route_disruption)
            for stop in stops: 
                stops_disruption = {}
                stops_disruption['disruption_id'] = disruption['disruption_id']
                for key in list(stop.keys()):
                    stops_disruption[key] = stop[key]
                all_stops_disruptions.append(stops_disruption)
            all_disruptions.append(cleaned_disruption)  
    return all_disruptions, all_route_disruptions, all_stops_disruptions


def transform_disruptions(df):
    df_all_disruptions = df
    df_all_disruptions = pd.DataFrame(all_disruptions)
    df_all_disruptions['hashed_id'] = two_column_hash(df_all_disruptions,'disruption_id','last_updated')
    df_all_disruptions = df_all_disruptions.drop(['combined','hashed'],axis=1)
    return df_all_disruptions 


def get_file_names_from_bucket(bucket,prefix,months_to_load,big_query_client):
    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    files_to_load =[]
    data_loader_status = {}
    latest_date = get_latest_file_loaded(big_query_client)
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
    meta_data = [{'load_date':today,'latest_file_loaded':latest_date}]
    return data_loader_status,files_to_load,meta_data

def read_files_from_bucket(data_loader_status):
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
key_path = "bigquery_key.json"

project_id = 'gcp-project'


def get_big_query_client(key_path): 
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    big_query_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    return big_query_client

table_id = "gcp-project.analytics.ptv_disruptions_meta_data"

def check_if_table_exists(big_query_client, table_id):
    try:
        big_query_client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        print("Table {} is not found.".format(table_id))
        return False
    
def get_latest_file_loaded(big_query_client):
    if check_if_table_exists(big_query_client,"gcp-project.analytics.ptv_disruptions_meta_data"):
        sql = """
        SELECT 
        MAX(latest_file_loaded) as latest_file
        FROM `gcp-project.analytics.ptv_disruptions_meta_data` """

        df_latest_file_loaded = big_query_client.query(sql).to_dataframe()
        df_latest_file_loaded.head()
        latest_file_loaded = df_latest_file_loaded['latest_file'][0]
        return latest_file_loaded
    return 202311010000

def write_to_bq(big_query_client,df,table_name,write_disposition="WRITE_APPEND"):
    # Save results as parquet
    # This result in a smaller file size 
    # and will keep intact your schema 
    try:
        table_id = "gcp-project.analytics."+table_name
        df.write_parquet("/my_data.parquet")
        print("------------ File added as a parquet ----------------")
        # Set configurations for the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition=write_disposition,
        )

        # Open the data a binary 
        with open("/my_data.parquet", "rb") as f:
            job = big_query_client.load_table_from_file(f, table_id, job_config=job_config)

        # Execute the job
        job.result()
    except Exception as e:
        # Log the exception if an error occurs
        logging.error(f"Error in transform_data: {str(e)}")
        raise  # Re-raise the exception to mark the task as failed

def transform_data():
    big_query_client = get_big_query_client(key_path)
    data_loader_status,files_to_load,meta_data = get_file_names_from_bucket(bucket=bucket,prefix=prefix,months_to_load=months_to_load,big_query_client=big_query_client)
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
