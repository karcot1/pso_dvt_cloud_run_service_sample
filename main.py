import subprocess
from flask import Flask
import os
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import bigquery
from google.auth.transport import requests
import google.auth
import math
import sys

import gcsfs
import pandas as pd
import datetime

app = Flask(__name__)

project_id = os.environ.get("PROJECT_ID")

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

@app.route('/', methods=['POST'])

def index():

    # try:
    #     get_credentials(project_id)
    # except Exception as e:
    #     print("Error getting TD credentials: ", e)

    try:
        create_connections()
    except Exception as e:
        print("Error executing DVT: ", e)

    try:
        execute_dvt()
    except Exception as e:
        print("Error executing DVT: ", e)
    
    return "Execution complete"


def create_connections():
    print('calling bash script to create connections')
    return_code = subprocess.call(['bash',"./connections.sh", "dataform-test-362521"])
    print ('return_code',return_code)
    return "create_connectons completed successfully"

# required for Teradata connections: pulls connection information from secret manager and saves as environment variables

# def get_credentials(BQprojectId):
#     client = secretmanager.SecretManagerServiceClient()
#     teradata_secret = f"projects/{BQprojectId}/secrets/tera-credentials/versions/latest"    
#     response = client.access_secret_version(name=teradata_secret)
#     payload= response.payload.data.decode("UTF-8")
#     tera_json=json.loads(payload)
#     for key,value in tera_json.items():
#         os.environ[key] = value

def partition_assessment(bq_table):

    # calculate partitions and parts per file needed based on table size for row hash validation
    # does not currently support custom query partitions - will need to specify partitioning features manually in CSV

    print('obtaining size of table')

    client = bigquery.Client()
    query = f"""SELECT COUNT(*) FROM {bq_table}"""

    partition_output = {}

    try:
        results = client.query(query).result()
        row_count = next(results)[0]
        print('table size: ' , str(row_count))
    except Exception as e:
        print('Error executing query: ', e)

    # throw error if TD table is greater than TD's upper limit for INT datatypes
    if row_count > 2147483647:
        raise Exception('This table size will exceed the Teradata upper limit for INT values and prevent DVT from running. Please filter your table into smaller subsets before executing.')

    if row_count >= 150000:
        partition_output["needs_partition"] = "Y"
        print('table will need partitioning')

        num_partitions = math.ceil(row_count / 50000)
        print('total number of partitions: ', num_partitions)
        if num_partitions > 10000:
            parts_per_file = math.ceil(num_partitions / 10000)
            print('number of partitions per YAML file: ', parts_per_file)
        else:
            parts_per_file = 1
            print('number of partitions per file: 1')
        partition_output["num_partitions"] = num_partitions
        partition_output["parts_per_file"] = parts_per_file
    else:
        partition_output["needs_partition"] = "N"
        print('table will not need partitioning.')
    
    return partition_output
    


# def invoke_cloud_run(yaml_file_path,source_DB,project_id,cloud_run_url,no_of_partitions, ppf):
#     AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
#     credentials, _ = google.auth.default(scopes=[AUTH_SCOPE])
#     credentials.refresh(requests.Request())      
   
#     oauth_token=credentials.token
#     authorization = f"Bearer {oauth_token}"
#     headers = {
#     "accept": "application/json",
#     "Authorization": authorization
#     }
#     #override_env_val = f'{{"overrides": {{"containerOverrides": [{{"env": [{{"name": "PROJECT_ID", "value": "{project_id}"}},{{"name": "CONFIG_YAML_PATH", "value": "{yaml_file_path}"}},{{"name": "SOURCE_DB", "value": "{source_DB}"}}]}}],"task_count":{no_of_partitions} }}}}'
#     override_env_val = f'{{"overrides": {{"containerOverrides": [{{"env": [{{"name": "PROJECT_ID", "value": "{project_id}"}},{{"name": "CONFIG_YAML_PATH", "value": "{yaml_file_path}"}},{{"name": "SOURCE_DB", "value": "{source_DB}"}}]}}] }}}}'
    
#     parallelism = no_of_partitions if int(no_of_partitions) < 50 else 49
#     tasks =    no_of_partitions if int(no_of_partitions) < 10000 else math.ceil(int(no_of_partitions)/2 )  
#     extract_cloud_run_job_name = cloud_run_url.split("/jobs/")
#     extract_cloud_run_job_name=extract_cloud_run_job_name[1].split(":")
#     print (extract_cloud_run_job_name[0])
#     gcloud_command =f"gcloud run jobs update {extract_cloud_run_job_name[0]} --region us-central1 --parallelism {parallelism} --tasks {tasks}"
#     print (gcloud_command)
    
#     try:
#         print ("before execution of command shell")
#         result = subprocess.run(gcloud_command,shell=True,capture_output=True,text=True)  
#         print (override_env_val)
#         if result.returncode == 0:
#             response=requests.post(cloud_run_url,headers=headers,data=override_env_val)   

#             if response.status_code == 200:
#                 print ("DVT with config complete")
#             else:
#                 print ("Request Failed with status code",response.status_code)  
#         else:
#             print ("failed to update parallelism")

#     except subprocess.CalledProcessError as e:
#         print(f"Error updating Cloud Run job: {e.stderr}")

def execute_dvt():
    print('Executing DVT')

    print('reading CSV from GCS file')
    df = pd.read_csv('gs://dvt_configs/dvt_executions.csv')
    for index, row in df.iterrows():
        print('current table: ' + row['target_table'])

        if row['validation_type'] == 'column':
            print('calling shell script for column validation')

            if pd.isna(row['count_columns']):
                return_code = subprocess.call(['bash',"./run_dvt.sh", "count", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],"'*'",row['output_table']])                
                print ('return_code',return_code)      
                if return_code !=0:
                    print ("Error executing DVT validations")
            else:
                return_code = subprocess.call(['bash',"./run_dvt.sh", "count", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['count_columns'],row['output_table']])                
                print ('return_code',return_code)      
                if return_code !=0:
                    print ("Error executing DVT validations")

        if row['validation_type'] == 'row_hash':
            partition_output = partition_assessment(row['target_table'])
            if partition_output['needs_partition'] == "N":
                print('calling shell script for row validation')

                if row['exclude_columns'] == 'Y':
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "row", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"Y",row['exclude_column_list'],row['output_table']])
                    print ('return_code',return_code)
                else:
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "row", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"N",row['output_table']])
                    print ('return_code',return_code)

            else:
                print('generating partition yamls')

                table_name = row['target_table'].split('.')[2]
                datetime_var = '{date:%Y-%m-%d_%H:%M:%S}'.format(date=datetime.datetime.now())
                gcs_location = 'gs://dvt_yamls/' + table_name + '/' + datetime_var

                if row['exclude_columns'] == 'Y':
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "partition", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"Y",row['exclude_column_list'],row['output_table'],partition_output['num_partitions'],partition_output['parts_per_file'],gcs_location])
                    print ('return_code',return_code)
                else:
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "partition", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"N",row['output_table'],partition_output['num_partitions'],partition_output['parts_per_file'],gcs_location])
                    print ('return_code',return_code)

        if row['validation_type'] == 'custom_query':
            print('executing custom sql validation')

            if row['partition'] == "N":
                print('calling shell script for custom query validation')
                if row['exclude_columns'] == 'Y':
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "custom_no_partition", row['source_conn'],row['target_conn'],row['primary_keys'],"Y",row['exclude_column_list'],row['source_sql_location'],row['target_sql_location'],row['output_table']])
                    print ('return_code',return_code)
                else:
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "custom_no_partition", row['source_conn'],row['target_conn'],row['primary_keys'],"N",row['source_sql_location'],row['target_sql_location'],row['output_table']])
                    print ('return_code',return_code)
            else:
                print('generating partition yamls for custom query validation')
                table_name = row['target_table'].split('.')[2]
                datetime_var = '{date:%Y-%m-%d_%H:%M:%S}'.format(date=datetime.datetime.now())
                gcs_location = 'gs://dvt_yamls/' + table_name + '/' + datetime_var

                if pd.isna(row['ppf']):
                    ppf = '1'
                else:
                    ppf = str(int(row['ppf']))

                if row['exclude_columns'] == 'Y':
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "custom_partition", row['source_conn'],row['target_conn'],row['primary_keys'],"Y",row['exclude_column_list'],row['source_sql_location'],row['target_sql_location'],row['output_table'],str(int(row['num_partitions'])),ppf,gcs_location])
                    print ('return_code',return_code)
                else:
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "custom_partition", row['source_conn'],row['target_conn'],row['primary_keys'],"N",row['source_sql_location'],row['target_sql_location'],row['output_table'],str(int(row['num_partitions'])),ppf,gcs_location])
                    print ('return_code',return_code)
    return "DVT executions completed"

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    