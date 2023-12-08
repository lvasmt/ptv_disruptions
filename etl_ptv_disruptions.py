import pandas as pd
import requests
import json
import hashlib
import pytz
import boto3
from hashlib import sha1
from hmac import new
from datetime import datetime,timezone

def run_ptv_dag():
    #Define the variables
    user_id = 'api_user_id'
    api_key = 'apikey'
    base_url = 'https://timetableapi.ptv.vic.gov.au/'
    today = datetime.today().strftime('%Y%m%d')
    utc_dt = datetime.now(timezone.utc)
    local_tz = "Australia/Melbourne"
    local_time = pytz.timezone(local_tz)
    local_dt = utc_dt.astimezone(local_time).strftime('%Y%m%d%H%M')
    s3_bucket = 'lnv-general-bucket/'

    def get_s3_client():
        with open('/aws_keys.json','r') as f: 
            aws_keys = json.load(f)
        s3_client = boto3.client('s3',aws_access_key_id=aws_keys['AWS_ACCESS_KEY'],aws_secret_access_key=aws_keys['AWS_SECRET_KEY'])
        return s3_client

    def upload_json_file(s3_client,data,file_name):
        s3_client.put_object(
            Body=json.dumps(data),
            Bucket='lnv-general-bucket',
            Key=file_name
        )

    def hasher(x):
        y = x.encode()
        hasher = hashlib.sha256()
        hasher.update(y)
        return hasher.hexdigest()

    def two_column_hash(df,col1,col2):
        df_copy = df
        df_copy['combined'] = df_copy[col1].astype(str)+df_copy[col2].astype(str)
        df_copy['hashed'] = df['combined'].apply(hasher)
        return df_copy['hashed']

    def get_endpoint(endpoint):
        endpoints = {'disruptions':'/v3/disruptions',
                    'routes':'/v3/routes'}
        return endpoints[endpoint]

    def construct_query(**kwargs):
        query_string = ''
        for query in kwargs.keys():
            query_string = query_string+('?' if query_string == '' else '&')+str(query)+'='+str(kwargs[query])
        return  query_string

    def get_url(endpoint,*args,**kwargs):
        devId = user_id
        key = api_key
        byte_key = bytes(key, "UTF-8")
        request = get_endpoint(endpoint) + construct_query(**kwargs)
        raw_request = request+'&devid={0}'.format(devId)
        signature = new(bytes(key, "UTF-8"), raw_request.encode(),  sha1).hexdigest()
        final_url = 'https://timetableapi.ptv.vic.gov.au'+raw_request+'&signature={1}'.format(devId, signature)
        return final_url

    def load_ptv_disruptions():
        s3_client=get_s3_client()
        response = requests.get(get_url('disruptions',disruption_status='current'))
        upload_json_file(s3_client,json.loads(response.text),'ptv_disruptions_'+local_dt+'.json')

    load_ptv_disruptions()
