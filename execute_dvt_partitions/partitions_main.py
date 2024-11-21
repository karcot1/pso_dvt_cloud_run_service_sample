import subprocess
from flask import Flask
import os
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import bigquery
import google.auth

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

# UNCOMMENT TO RUN AS SERVICE:
# app = Flask(__name__)
# @app.route('/', methods=['POST'])


def main():

    print('collecting input parameters...')

    # task_index = os.getenv("CLOUD_RUN_TASK_INDEX")

    project_id = os.environ.get("PROJECT_ID")
    print("project_id: " + project_id)

    config_yaml_path = os.environ.get("CONFIG_YAML_PATH")
    print("yaml_fullpath: " + config_yaml_path)

    # yaml_file_name = config_yaml_path + task_index.rjust(4, "0") + ".yaml"
    # label = f"yaml_file={yaml_file_name}"

    # os.environ["YAML_FILE_LABEL"] = label

    # yaml_fullpath = config_yaml_path

    # get_credentials(bq_project_id) # required for TD connections

    print('creating connections')
    subprocess.call(['bash', "./partition_connections.sh", project_id])

    subprocess.call(['bash', "./execute_dvt_partition_yamls.sh", config_yaml_path, project_id])

# # required for Teradata connections: pulls connection information from secret manager and saves as environment variables

# # def get_credentials(BQprojectId):
# #     client = secretmanager.SecretManagerServiceClient()
# #     teradata_secret = f"projects/{BQprojectId}/secrets/tera-credentials/versions/latest"    
# #     response = client.access_secret_version(name=teradata_secret)
# #     payload= response.payload.data.decode("UTF-8")
# #     tera_json=json.loads(payload)
# #     for key,value in tera_json.items():
# #         os.environ[key] = value

if __name__ == "__main__":
    # UNCOMMENT TO RUN AS SERVICE:
    # app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    
    #UNCOMMENT TO RUN AS JOB:
    main()

    