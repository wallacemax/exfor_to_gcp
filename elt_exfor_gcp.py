# Use CouchDB to create a CouchDB client
# from cloudant.client import CouchDB
# client = CouchDB(USERNAME, PASSWORD, url='http://127.0.0.1:5984', connect=True)

# Use Cloudant to create a Cloudant client using account
from cloudant.client import CouchDB
from google.cloud import storage
import json
from dask import dataframe as dd
import time
from multipro import multipro_helper
import numpy as np

datasetids='datasetids'

def get_json_put_json(documentnumber):
    this_start = time.time()
    USERNAME="admin"
    PASSWORD="password"
    DB_NAME="exfor"
    KEYS_PATH="/Users/maxwallace/bkup/codes/not/dataengineering/exfor_to_gcp/nuclear-data-raw-4035322a0d52.json"
    bucket_name="exfor_lake"
    
    client = CouchDB(USERNAME, PASSWORD, url="http://127.0.0.1:5984", connect=True)

    # Define the end point and parameters
    end_point = '{0}/{1}'.format(client.server_url, f'{DB_NAME}/{documentnumber}')
    params = {'include_docs': 'true'}

    # Issue the request
    response = client.r_session.get(end_point, params=params)
    
    # Disconnect from the server
    client.disconnect()
    
    if 'missing' in response.json():
        print('missing')
        return
    
    #with open(f'/Users/maxwallace/bkup/codes/not/dataengineering/exfor_to_gcp/x4_json/{documentnumber}.json', 'w', encoding='utf-8') as f:
        #json.dump(response.json(), f, ensure_ascii=False, indent=4)    
    
    #put the json into the cloud
    storage_client = storage.Client.from_service_account_json(KEYS_PATH)
    BUCKET= storage_client.get_bucket(bucket_name)

    blob = BUCKET.blob(documentnumber)
    blob.upload_from_string(
        data=str(response.json()),
        content_type="application/json"
        )
    print(f"{documentnumber} in {time.time()-this_start} seconds")
    
if __name__ == "__main__":
    start = time.time()
    datasetids = list(dd.read_csv(datasetids, dtype=str)['docid'])
    print(f"read csv in {time.time() - start} seconds")
    
    multipro_helper(get_json_put_json, datasetids[:1000], cpus=None, threads=6)