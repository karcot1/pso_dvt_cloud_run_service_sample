import subprocess
from flask import Flask
import os
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import bigquery
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

            if row['partition'] == "N":
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

                if pd.isna(row['ppf']):
                    ppf = '1'
                else:
                    ppf = str(int(row['ppf']))

                if row['exclude_columns'] == 'Y':
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "partition", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"Y",row['exclude_column_list'],row['output_table'],str(int(row['num_partitions'])),ppf,gcs_location])
                    print ('return_code',return_code)
                else:
                    return_code = subprocess.call(['bash',"./run_dvt.sh", "partition", row['source_conn'],row['target_conn'],row['source_table'],row['target_table'],row['primary_keys'],"N",row['output_table'],str(int(row['num_partitions'])),ppf,gcs_location])
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
    