import subprocess
# from flask import Flask
# import os
# from google.cloud import secretmanager
# from google.cloud import storage
# from google.cloud import bigquery
# import google.auth
# import csv
# from dotenv import load_dotenv
# import requests
# from google.auth.transport import requests as auth_request
# import math
# import sys
# import datetime 
# import json
# from concurrent.futures import ThreadPoolExecutor,as_completed
# from io import StringIO
# import teradatasql
# import pytz
# import pandas as pd
# import gcsfs
import pandas as pd
import datetime


# #app = Flask(__name__)
# #@app.route("/", methods=["POST", "GET"])

# cloud_run_name = os.getenv("CLOUD_RUN_EXECUTION")
# print ('cloud_run_instance_name',cloud_run_name)

# # tera_temp_schema=os.environ.get("TERADATA_TEMP_SCHEMA")
# # print ('tera_temp_schema',tera_temp_schema) 

# # bq_temp_schema=os.environ.get("BQ_TEMP_SCHEMA")
# # print ('bq_temp_schema',bq_temp_schema) 

# # teradata_secret_path=os.environ.get("TERADATA_CREDENTIALS_SECRET_PATH")
# # print ('teradata_secret_path',teradata_secret_path) 

# project_id =os.environ.get("PROJECT_ID")
# print("project_id", project_id)
    
# config_bucket_name=os.environ.get("CONFIG_BUCKET_NAME") 
# print("bucket_name", config_bucket_name)  
    
# config_file_name=os.environ.get("CONFIG_FILE_NAME")    
# print("source_file", config_file_name)    
    
# bq_results_table=os.environ.get("BQ_RESULTS_TABLE")
# print ('bq_results_table',bq_results_table)    
    
# validation_type=os.environ.get("VALIDATION_TYPE")
# print ('validation_type',validation_type)
    
    
# partition_sql_config_file_path=os.environ.get("PROCESSING_AREA_GCS_BUCKET_NAME")
# print ('partition_config_file_path',partition_sql_config_file_path)
    

# cloud_run_url=os.environ.get("CLOUD_RUN_URL")
# print ('cloud_run_url',cloud_run_url) 

# bq_results_table_dset = bq_results_table.split(".")[1]

# run_date_utc = datetime.datetime.utcnow()
# est_timezone = pytz.timezone("US/Eastern")
# currest_est_time = run_date_utc.astimezone(est_timezone)
# run_date = int(currest_est_time.strftime("%Y%m%d"))

# def download_config(config_bucket_name,config_file_name,project_id):
#     storage_client = storage.Client(project=project_id)
#     bucket = storage_client.bucket(config_bucket_name)
#     blob = bucket.blob(config_file_name)
#     content=blob.download_as_text()
#     lines=content.decode('utf-8').split('\n')
#     csv_reader = csv.reader(lines)
#     next(csv_reader)
#     return content

# def main():       

#     # This is used when running locally. Gunicorn is used to run the
#     # application on Cloud Run. See entrypoint in Dockerfile.
#     #app.run(host="127.0.0.1", port=PORT, debug=True)
   
    
  
#     AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
#     CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])     
    
#     chunk_size = 5
#     max_workers = 1
   
#     primary_keys=""
#     tera_connection=get_credentials(project_id)
    
#     try:
#         print('establish connections ')
#         return_code = subprocess.call(['bash',"./dvt_connections.sh",  project_id]) #,shell=True               
#         print ('return_code',return_code)      
#         if return_code !=0:
#             print ("Error establishing source and target connections")                               
#     except subprocess.CalledProcessError as e:
#         print ("Error establishing source and target connections :{e}")
#         raise 

#     if validation_type == "count":
#         csv_content=download_config(config_bucket_name,config_file_name,project_id)
#         futures=[]
#         with ThreadPoolExecutor(max_workers=max_workers) as executor:
#             for chunk in read_lines_in_chunks(csv_content,chunk_size):            
#                 futures.append(executor.submit(count_validation,chunk,project_id,bq_results_table))            
            
#             results =[]
#             for future in as_completed(futures):
#                 result = future.result()
#                 results.append(result)
#             #print(f"rows processed {result} rows")
#         total_rows = sum(results)
#         print(f"Total rows processed {total_rows}")                     
#     else:
#         print ('inside row validation')
#         row_validation(config_bucket_name,config_file_name,project_id,bq_results_table,partition_sql_config_file_path,cloud_run_url)
            
               
#         #archive_files(destination_bucket,archive_bucket)

# def normalize(str_data):
#     return str_data.strip()

# def get_credentials(project_id):
#     client = secretmanager.SecretManagerServiceClient()
#     teradata_secret = teradata_secret_path 
#     print ('teradata_secret path', teradata_secret) 
#     response = client.access_secret_version(name=teradata_secret)
#     payload= response.payload.data.decode("UTF-8")
#     tera_json=json.loads(payload)
#     for key,value in tera_json.items():
#         os.environ[key] = value

#     new_folder_path=os.path.join("./","sql")
#     os.makedirs(new_folder_path,exist_ok=True)
#     print(f"Folder {new_folder_path} created successfully ")  
#     # initialize Tera Credentials 
    
    
            
#         #raise
    
    
        

# def invoke_cloud_run(yaml_file_path,source_DB,project_id,cloud_run_url,no_of_partitions):
#     AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
#     credentials, _ = google.auth.default(scopes=[AUTH_SCOPE])
#     credentials.refresh(auth_request.Request())    
#     print ('inside 2nd cloud run invocation')        
   
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
    
#     # ADD STATUS CHECK FOR CLOUD RUN COMPLETION 


# def read_lines_in_chunks(data,chunk_size):
#     print ('inside read_lines in chunks')
#     reader = csv.reader(StringIO(data))
#     next(reader,None)
#     chunk=[]
#     for row in reader:
#         #print(row)
#         chunk.append(row)
#         if len(chunk) == chunk_size:
#             yield chunk
#             chunk =[]
#     if chunk:
#         yield chunk    
    
# def count_validation(chunk,project_id,bq_results_table):
#     for row in chunk:                
#         try:
#             source_DB=normalize(row[0])
#             target_DB=normalize(row[1])
#             source_view_dataset=normalize(row[3])
#             source_view_name=normalize(row[4])
#             target_proj=normalize(row[5])
#             target_dataset=normalize(row[6])                
#             target_tbl=normalize(row[7])
#             count_validation_flag=normalize(row[8])            
#             filters = normalize(row[12]) or "None"
#             labels = normalize(row[17]) or "None"
            
#             #print (row)        
#             if count_validation_flag == "Y":                    
#                 print (target_tbl, " ", count_validation_flag)                
#                 tbls_list= source_view_dataset +"." + source_view_name +"=" + target_proj + "." + target_dataset+ "." + target_tbl
                
#                 sql = f"select coalesce(max(run_iteration),0) run_iteration from {bq_results_table_dset}.bq_tdexit_dvt_latest_run_tracker where source_table_name ='{source_view_dataset}.{source_view_name}' and target_table_name = '{target_proj}.{target_dataset}.{target_tbl}' and validation_type = 'count'"
#                     # get latest run id 
                    
#                 latest_run_id = dvt_meta_update(source_view_dataset,source_view_name,target_proj,target_dataset, target_tbl,bq_results_table_dset,sql,'get_max_runid')
#                 print ('latest_run_id',latest_run_id)
#                 tbls_list_labels= f"Tables={source_view_dataset}.{source_view_name}:{target_dataset}.{target_tbl},run_date={run_date}-{latest_run_id},cloud_run_task={cloud_run_name}"
#                 print ('tbls_list_labels',tbls_list_labels)
                
#                 labels_derived = tbls_list_labels + (f",{labels}" if labels != "None" else '')
                             
#                 try:
#                     print('call dvt for counts for ', tbls_list)
#                     return_code = subprocess.call(['bash',"./run_dvt.sh", source_DB, project_id,tbls_list,"None",bq_results_table,"count","None",filters,"None","None","None","None",labels_derived,target_DB])                
#                     print ('return_code',return_code)      
#                     if return_code !=0:
#                         print ("Error executing DVT validations") 
#                     else:
#                         # update tracker table with latest run id 
#                         sql =f"""
#                         insert into {bq_results_table_dset}.bq_tdexit_dvt_latest_run_tracker 
#                         Values ('{source_view_dataset}.{source_view_name}','{target_proj}.{target_dataset}.{target_tbl}',{run_date},{latest_run_id},'run_date={run_date}-{latest_run_id}','count');
#                         """
#                         status = dvt_meta_update(source_view_dataset,source_view_name,target_proj,target_dataset, target_tbl,bq_results_table_dset,sql,'insert_latest_runid')                               
                                              
#                 except subprocess.CalledProcessError as e:
#                     print ("Error executing DVT validations :{e}")
#                     raise     
#         except IndexError:
#             print("last row reached")
#     return len(chunk)


   

# def call_dvt_shell(source_DB, project_id,tbls_list,primary_keys,bq_results_table,validation_type,compare_fields,filters,yaml_file_path,no_of_partitions,src_file,tgt_file,labels,target_DB):
#     try:
#         print(f"call dvt for {tbls_list}")
#         #print("./run_dvt.sh", source_DB, project_id,tbls_list,primary_keys,bq_results_table,validation_type,compare_fields,filters,yaml_file_path,no_of_partitions,src_file,tgt_file)
#         return_code = subprocess.call(['bash',"./run_dvt.sh", source_DB, project_id,tbls_list,primary_keys,bq_results_table,validation_type,compare_fields,filters,yaml_file_path,no_of_partitions,src_file,tgt_file,labels,target_DB]) #,shell=True to run locally
#         print ('return_code',return_code)
#         return return_code
                                
#     except subprocess.CalledProcessError as e:
#         print ("Error executing DVT validations :{e}")
#         raise


# def create_sql_files(target_project,target_schema,target_tablename,source_table,source_project,source_schema,source_view_name,dbtype,destination_bucket,file_path,pks,filter_columns,xref_columnname,xref_tablename,partition_data,labels,filters):
   
#     print (dbtype)
#     if dbtype == 'Teradata':
#         source_to_compare = source_schema + '.'  + source_view_name
#         source_full_tbl_name = source_table        

#     target_to_compare = target_project + '.' + target_schema + '.'  + target_tablename
#     bqclient = bigquery.Client()
#     query = f"""
#             SELECT count(*) as tot_columns FROM `{target_project}.region-us.INFORMATION_SCHEMA.COLUMNS` info_schema 
#             WHERE concat(table_catalog,".",table_schema,".",table_name) = '{target_to_compare}'   
#         """
#     #print (query)        
#     run_qry = bqclient.query(query)
#     result_row = run_qry.result()
#     for row in result_row:
#         total_columns=int(row[0])
#     query = f"""
#             SELECT coalesce(string_agg(column_name_to_exclude,","),'None') as excl_list from `{target_project}.{bq_results_table_dset}.bq_tdexit_dvt_exclusion_list` 
#             WHERE concat(table_catalog,".",table_schema,".",table_name) = '{target_to_compare}' and column_to_exclude_flag = 'Y'  
#         """
   
#     run_qry = bqclient.query(query)
#     result_row = run_qry.result()
#     exclusion_list ="None"
#     for row in result_row:
#         exclusion_list=str(row[0])       
            
    
#     print (exclusion_list)

#     #print (total_columns)
#     cntr = 0 
#     batch_size = 70

#     pks_filters = (pks if pks != "None" else '') + (f",{filter_columns}" if filter_columns != "None" else '')
#     print ("pks_filters", pks_filters)
#     #print (filters)

#     print ('file_path',file_path)
    
#     gcs_uri=[]
#     if partition_data == "N":
#         view_name = 'None'
#         #if total_columns <= 100 :
#         cntr = 0
#         batch_size = 70
#         gcs_uri=[]

#         while cntr < total_columns:

#             if dbtype == 'BigQuery':
#                 gcs_uri_temp = f"{file_path}{target_schema}_{target_tablename}_tgt_{cntr}.sql"
#                 result_query = get_query(dbtype,pks_filters,target_to_compare,'NA',target_project,cntr,batch_size,exclusion_list,view_name,xref_columnname,xref_tablename,filters)
#                 gcs_uri.append(gcs_uri_temp)
#             else:
#                 gcs_uri_temp = f"{file_path}{target_schema}_{target_tablename}_src_{cntr}.sql"
#                 result_query = get_query(dbtype,pks_filters,source_to_compare,source_full_tbl_name,target_project,cntr,batch_size,exclusion_list,view_name,xref_columnname,xref_tablename,filters)
#                 gcs_uri.append(gcs_uri_temp)

#             gcs_client =storage.Client()
#             bucket = gcs_client.bucket(destination_bucket)    
#             blob=bucket.blob(gcs_uri_temp)
#             blob.upload_from_string(result_query,content_type="application/sql")

#             cntr = cntr+batch_size
       
#         return gcs_uri
         
#     else:
#         cntr = 0 
#         batch_size = 70
#         gcs_uri=[]
#         if labels == 'None':
#             labels_value = "all"
#         else:
#             labels_value = labels.split("=")[1]     
#         while cntr < total_columns: 
#             if dbtype == 'BigQuery':
#                 view_name= f"{bq_temp_schema}.{target_tablename}_tgt_{cntr}_ds{labels_value}"
#                 print ('taget view name' ,view_name)
#                 result_query = get_query(dbtype,pks_filters,target_to_compare,'NA',target_project,cntr,batch_size,exclusion_list,view_name,xref_columnname,xref_tablename,filters)               
#                 gcs_uri.append(view_name)                
                
                
#             else:
#                 view_name= f"{tera_temp_schema}.{target_tablename}_src_{cntr}_ds{labels_value}"
#                 print ('source view name' ,view_name)
#                 result_query = get_query(dbtype,pks_filters,source_to_compare,source_full_tbl_name,target_project,cntr,batch_size,exclusion_list,view_name,xref_columnname,xref_tablename,filters)
#                 gcs_uri.append(view_name)               
         
        
#             cntr=cntr+batch_size
            
#         return gcs_uri

# def get_query(dbtype,pks_filters,table_to_compare,source_full_tbl_name,target_project,cntr,batch_size,exclusion_list,view_name,xref_columnname,xref_tablename,filters):
              
#     print (dbtype)
#     result_string = "None"
#     if dbtype == 'BigQuery':
                          
#         query = f""" 
            
#                 with t as 
#                 (
#                 select column_names,offset + (-9998) as ordinal_position  from
#                   UNNEST( SPLIT('{pks_filters}',',')) as column_names
#                     with OFFSET as offset
#                     ),
#                 excl_list as 
#                 (
#                   select column_names  from
#                   UNNEST( SPLIT('{exclusion_list}',',')) as column_names
#                 )
                
#                 select
#                     case when '{view_name}' = 'None' then               
#                         "select " || column_names || " from " || "{table_to_compare} a where {filters}" 
#                     else "create or replace view {view_name} as select " || column_names || " from " || "{table_to_compare} a where {filters}"                        
#                     end 
#                     ||

#                     case when '{xref_columnname}' <> 'None' then 
#                         ' LEFT JOIN bq_processing_area.tdexit_cross_ref_table xref on xref.bq_{xref_columnname} = a.{xref_columnname} '
#                     else '' 

#                     end as qry                  
#                 from 
#                 (
#                 select   STRING_AGG(column_names,',' order by ordinal_position)  as column_names
#                 from
#                 (
                                                  
#                 select 
#                    case    
#                         when data_type <> 'STRING' then column_name 
#                         when data_type = 'STRING' then "trim(regexp_replace(" || column_name || ",r'\\\\x00|\\\\x01|\\\\x02|\\\\x0d|\\\\x0a', '')) as " || column_name
#                     else column_name
#                     end
#                  as column_names,
#                  ordinal_position
            
#                 from 
#                     `{target_project}.region-us.INFORMATION_SCHEMA.COLUMNS` info_schema   
#                 WHERE 
#                     concat(table_catalog,".",table_schema,".",table_name) = '{table_to_compare}'                   
#                 and ordinal_position between {cntr} + 1 and {cntr} + {batch_size}
            
#                 and column_name not in  (
#                     select column_names from t
#                     union all
#                     select column_names from excl_list
#                     ) 
                    
#                     UNION ALL

#                     SELECT case when '{xref_columnname}' = column_names  then 'xref.td_{xref_columnname} as ' ||  column_names
#                         else column_names end column_names,ordinal_position
#                     from t
            
#                     ) /* string agg */        
#                 );
#                 """
      
        
#         bqclient = bigquery.Client()
#         run_qry = bqclient.query(query)
#         result_row = run_qry.result()
#         for row in result_row:
#             result_string=str(row[0])
#             if view_name != 'None':
#                 run_qry = bqclient.query(result_string)
#                 result_row = run_qry.result()
        
#     else:
#         try:
#             tera_connection=teradatasql.connect(host=os.environ["TERA_HOST"],user=os.environ["TERA_USERNAME"],password=os.environ["TERA_PASSWORD"])
           
#             print('connection established',tera_connection)  
#             filters = filters.replace("'","''")
#             query = f""" 
            
#                with pks as 
#                 (
#                 SELECT columnnames,tokennum + (-9999) as ordinal_position FROM TABLE (strtok_split_to_table( 1, '{pks_filters}', ',')
#                 RETURNS (Id INTEGER, tokennum integer, columnnames varchar(50)character set unicode) ) as d1
#                 ),
#                 excl_list as 
#                 (
#                 SELECT columnnames FROM TABLE (strtok_split_to_table( 1, '{exclusion_list}', ',')
#                 RETURNS (Id INTEGER, tokennum integer, columnnames varchar(50)character set unicode) ) as d2
#                 )
#                 select 
#                 case when '{view_name}' = 'None' then 
#                 'SELECT ' ||  TRIM(TRAILING ',' FROM XMLAGG((columnnames || ',') order by ordinal_position) (VARCHAR(30000))) ||  ' from {table_to_compare} where {filters} '
#                 else 'replace view {view_name} as LOCKING ROW FOR ACCESS SELECT ' ||  TRIM(TRAILING ',' FROM XMLAGG((columnnames || ',') order by ordinal_position) (VARCHAR(30000))) ||  ' from {table_to_compare} where {filters} '
#                 end as qry    
#                     from 
#                     (
#                     select  
#                     case 
#                         when columntype in ('CV','CF') and columnlength < 2 then 'TRIM(' || columnname || ') as ' ||   columnname
#                         when columntype in ('CV','CF') and columnlength >= 2 then 'trim(cast(otranslate(otranslate(oTranslate(' ||
#                         columnname || ',SUBSTRING( '|| columnname|| ',   TRANSLATE_CHK(' || 
#                     columnname || ' USING LATIN_TO_UNICODE),1),translate('''' using unicode_to_latin)),x''01'' x''0A'' x''0B'' x''0C'' x''0D'',''''),  x''00'' x''09'','' '') as varchar(' || trim(columnlength) || ') )) as ' || columnname 
#                     else columnname
#                     end as columnnames,  ordinal_position

#                     from
#                     (select trim(databasename) databasename ,trim(tablename) tablename, trim(columnname) columnname,
#                     trim(columntype) as columntype, cast(trim(columnlength) as integer) as columnlength,
#                     columnid - 1024  as ordinal_position 
#                     from
#                     dbc.columns
#                     where   trim(databasename) || '.' || trim(tablename)= '{source_full_tbl_name}'
                                    
#                     and ordinal_position between {cntr} + 1 and {cntr} + {batch_size}
#                     and TRIM(columnname) NOT IN 
#                     (select columnnames from pks 
#                         union all
#                     select columnnames from excl_list )
#                     )a

#                 union all 

#                 SELECT columnnames,ordinal_position from pks

#                     ) fnl
#             """
#             print (query)
#             try:
#                 print ('preparing query')                
#                 cursor=tera_connection.cursor()
#                 cursor.execute(query)
#                 rows=cursor.fetchall()
#                 cursor.close()
#                 print ('query prepared')            
#                 for row in rows:
#                     result_string = str(row[0])
#                     print (result_string)
#                     if view_name != 'None' :
#                         print (f"creating view {view_name}")                        
#                         cursor=tera_connection.cursor()
#                         cursor.execute(result_string)                      
#                         cursor.close()
#                         tera_connection.close()
#                         print (f"view {view_name} created in Teradata")                                                                      
                
                
#             except teradatasql.OperationalError:
#                 print('\n--ERROR: Teradata query execution fail.')
#         except:
#             print('teradata query generation failed')
            
#     return result_string
            
  


# def row_validation(config_bucket_name,config_file_name,project_id,bq_results_table,partition_sql_config_file_path,cloud_run_url):
    
    
#     storage_client = storage.Client(project=project_id)
#     bucket = storage_client.bucket(config_bucket_name)
#     blob = bucket.blob(config_file_name)    
#     content=blob.download_as_string()
#     lines=content.decode('utf-8').split('\n')
#     csv_reader =csv.reader(lines)
#     next(csv_reader)    
#     primary_keys=""        
    
#     for row in csv_reader:
#         tbls_list=""
#         try:
#             #print (row)                
#             source_DB=normalize(row[0])
#             target_DB=normalize(row[1])
#             source_tbl=normalize(row[2]) or "None"
#             source_view_dataset=normalize(row[3])
#             source_view_name=normalize(row[4])
#             target_proj=normalize(row[5])
#             target_dataset=normalize(row[6])                
#             target_tbl=normalize(row[7])
#             row_validation_ind=normalize(row[9])
#             primary_keys =normalize(row[10]) or "None"
#             compare_fields =normalize(row[11]) or "None"
#             filters = normalize(row[12]) or "None"
#             generate_sql_ind = normalize(row[13]) or "N"
#             partition_data = normalize(row[14])  or "N"              
#             no_of_partitions = str(normalize(row[15]))  or "2"
#             filter_columns = normalize(row[16])  or "None"
#             labels = normalize(row[17])  or "None"
#             xref_columnname = normalize(row[18])  or "None"
#             xref_tablename = normalize(row[19])  or "None"
#             #print('each row')
#             #print (generate_sql_ind)
            
                
#             yaml_file_path = "None"   
#             src_sql_full_path = "None"     
#             tgt_sql_full_path  = "None"
#             src_file_gcs_location ="None"             
#             tgt_file_gcs_location="None"
#             config_file_path="None"
#             source_view_names =[]
#             target_view_names=[]
#             if row_validation_ind == "Y":       
             
#                 tbls_list= source_view_dataset +"." + source_view_name +"=" + target_proj + "."+ target_dataset+ "."+ target_tbl 
#                 #tbls_list_labels= f"Tables={source_view_dataset}.{source_view_name}:{target_dataset}.{target_tbl}"
#                 #labels_derived = tbls_list_labels + (f",{labels}" if labels != "None" else '')                

#                 if  primary_keys != "None":   
#                     sql = f"select coalesce(max(run_iteration),0) run_iteration from {bq_results_table_dset}.bq_tdexit_dvt_latest_run_tracker where source_table_name ='{source_view_dataset}.{source_view_name}' and target_table_name = '{target_proj}.{target_dataset}.{target_tbl}' and validation_type = 'row'"
#                     # get latest run id 
                    
#                     latest_run_id = dvt_meta_update(source_view_dataset,source_view_name,target_proj,target_dataset, target_tbl,bq_results_table_dset,sql,'get_max_runid')
#                     print ('latest_run_id',latest_run_id)                  
#                     tbls_list_labels= f"Tables={source_view_dataset}.{source_view_name}:{target_dataset}.{target_tbl},run_date={run_date}-{latest_run_id},cloud_run_task={cloud_run_name}"
                       
#                     labels_derived = tbls_list_labels + (f",{labels}" if labels != "None" else '') 
#                     #print ("inside valid Pk")
#                     if generate_sql_ind == "Y" and partition_data == "N" : #Generate sql for comparison                       
                                          
                        
#                         print ('create sql file')
                        
#                         target_tbl_full_path = target_proj + "."+ target_dataset+ "."+ target_tbl                        
#                         destination_bucket = partition_sql_config_file_path 
#                         sql_full_path = f"dvt_sql_files/{target_tbl_full_path}/" 
#                         print ('sql_full_path',sql_full_path)

#                         if source_tbl == "None":
#                             print(f"Source table name is not provided to generate the SQL for the table {target_tbl_full_path}")
#                             continue
                        
#                         source_gcs_uri = create_sql_files(target_proj,target_dataset,target_tbl,source_tbl,source_DB,source_view_dataset,source_view_name,source_DB,destination_bucket,sql_full_path,primary_keys,filter_columns,xref_columnname,xref_tablename,partition_data,labels,filters)
#                         #source_gcs_uri= f"gs://{destination_bucket}/{source_gcs_uri}"
                        
#                         #tgt_sql_full_path = f"dvt_sql_files/{target_tbl_full_path}/"                         
                            
#                         target_gcs_uri=create_sql_files(target_proj,target_dataset,target_tbl,target_DB,'NA',source_view_dataset,source_view_name,target_DB,destination_bucket,sql_full_path,primary_keys,filter_columns,xref_columnname,xref_tablename,partition_data,labels,filters)   
#                         #target_gcs_uri= f"gs://{destination_bucket}/{target_gcs_uri}"                        
                    
                                                
#                        # Read through the files to run validations based on sql 
                            
                                                
#                         if compare_fields != "None": 
#                             validation_type = "column_comparison"
#                         else:
#                             validation_type = "rowhash"
#                         total_queries = len(source_gcs_uri)
#                         for i in range(total_queries):
#                             source_vw_iterator = source_gcs_uri[i]
#                             target_vw_iterator = target_gcs_uri[i]
#                             source_gcs_uri_iterator= f"gs://{destination_bucket}/{source_vw_iterator}"
#                             target_gcs_uri_iterator= f"gs://{destination_bucket}/{target_vw_iterator}"  
#                             print ('source_gcs_uri',source_gcs_uri_iterator)
#                             print ('target_gcs_uri',target_gcs_uri_iterator)
#                             sql_file_labels=source_vw_iterator.split("/")[2]
#                             tgt_file_labels=target_vw_iterator.split("/")[2]
#                             file_labels=f"sql_files={sql_file_labels}:{tgt_file_labels}"
#                             labels_derived = tbls_list_labels + "," + file_labels + (f",{labels}" if labels != "None" else '')                            
                                                       
#                             return_code=call_dvt_shell(source_DB, project_id,tbls_list,primary_keys,bq_results_table,validation_type,compare_fields,filters,yaml_file_path,no_of_partitions,source_gcs_uri_iterator,target_gcs_uri_iterator,labels_derived,target_DB)
#                             if return_code !=0:
#                                 print ("Error executing DVT validations")
#                                 continue
                                
                    
#                     if generate_sql_ind == "Y" and partition_data == "Y" :

#                         if source_tbl == "None":
#                             print(f"Source table name is not provided to generate the SQL for the table {target_proj}.{target_dataset}.{target_tbl}")
#                             continue
#                         source_view_names = create_sql_files(target_proj,target_dataset,target_tbl,source_tbl,source_DB,source_view_dataset,source_view_name,source_DB,'None','None',primary_keys,filter_columns,xref_columnname,xref_tablename,partition_data,labels,filters)
#                         target_view_names = create_sql_files(target_proj,target_dataset,target_tbl,target_DB,'NA',source_view_dataset,source_view_name,target_DB,'None','None',primary_keys,filter_columns,xref_columnname,xref_tablename,partition_data,labels,filters)
#                         total_views = len(source_view_names)
#                         for i in range(total_views):
#                             source_vw_iterator = source_view_names[i]
#                             target_vw_iterator = target_view_names[i]
#                             tbls_list_partition= f"{source_vw_iterator}={target_proj}.{target_vw_iterator}"
                            
#                             validation_type ="partition"                                
#                             yaml_file_path=f"gs://" + partition_sql_config_file_path + "/dvt_config_files/row/"
#                             print ("yaml_file_path",yaml_file_path)                                               
                             
                           
#                             config_file_path= yaml_file_path + source_vw_iterator + "/"
#                             return_code=call_dvt_shell(source_DB, project_id,tbls_list_partition,primary_keys,bq_results_table,validation_type,compare_fields,filters,yaml_file_path,no_of_partitions,"None","None",labels_derived,target_DB)
#                             if return_code !=0:
#                                 print ("Error executing DVT validations")            
#                                 #sys.exit(return_code)                            
                            
#                             if return_code == 0 and partition_sql_config_file_path != "None" :
#                                 print ('partitions created, invoking 2nd cloud run job', config_file_path)                          
#                                 invoke_cloud_run(config_file_path,source_DB,project_id,cloud_run_url,no_of_partitions)
                    
#                     if generate_sql_ind != "Y":
#                         if partition_data == "Y"  and partition_sql_config_file_path != "None" :   #Generate partition files for 
#                             validation_type ="partition"                                
#                             yaml_file_path=f"gs://" + partition_sql_config_file_path + "/dvt_config_files/row/"
#                             print ("yaml_file_path",yaml_file_path)                                                    
                              
#                             source_full_path= f"{source_view_dataset}.{source_view_name}"  
#                             config_file_path= f"{yaml_file_path}{source_full_path}/"                                                                               
                        
#                         elif compare_fields != "None": # individual column comparison    
#                             validation_type = "column_comparison"                               
                            
#                         else: # defaults to row hash
#                             validation_type = "rowhash" 
                                
#                         return_code=call_dvt_shell(source_DB, project_id,tbls_list,primary_keys,bq_results_table,validation_type,compare_fields,filters,yaml_file_path,no_of_partitions,"None","None",labels_derived,target_DB)
#                         if return_code !=0:
#                             print ("Error executing DVT validations")            
#                             #sys.exit(return_code)                            
                            
#                         if return_code == 0 and partition_data == "Y"  and partition_sql_config_file_path != "None" :
#                             print ('partitions created, invoking 2nd cloud run job', config_file_path)                          
#                             invoke_cloud_run(config_file_path,source_DB,project_id,cloud_run_url,no_of_partitions)                

#                     if return_code == 0 :
#                         sql =f"""
#                         insert into {bq_results_table_dset}.bq_tdexit_dvt_latest_run_tracker 
#                         Values ('{source_view_dataset}.{source_view_name}','{target_proj}.{target_dataset}.{target_tbl}',{run_date},{latest_run_id},'run_date={run_date}-{latest_run_id}','row');
#                         """
#                         status = dvt_meta_update(source_view_dataset,source_view_name,target_proj,target_dataset, target_tbl,bq_results_table_dset,sql,'insert_latest_runid')        
                      
                        
#                 else:
#                     raise ValueError("Row hash specified without Primary Key")
#                     #sys.exit(return_code)
#         except IndexError:
#             print("last row reached")  

# def dvt_meta_update(source_view_dataset,source_view_name,target_proj,target_dataset, target_tbl,dset,sql,indicator): 
   
#     if indicator == 'get_max_runid':
#         print ('get max run id')
#         client = bigquery.Client(project=project_id)
#         job_config = bigquery.QueryJobConfig(        
#             priority=bigquery.QueryPriority.BATCH
#             )    
#         query_job = client.query(sql, job_config=job_config)     
#         results = query_job.result()
#         for row in results:
#             iteration=int(row[0])
#         iteration = iteration + 1   
#         #print ('iteration',iteration)
#         return iteration
#     else:
#         print ('update max run id')    
#         client = bigquery.Client(project=project_id)
#         job_config = bigquery.QueryJobConfig(        
#             priority=bigquery.QueryPriority.BATCH
#             )    
#         query_job = client.query(sql, job_config=job_config)     
#         results = query_job.result()
#         return 0


if __name__ == "__main__":    
    # with open('dvt_executions.csv') as csv_file:
        # csv_reader = csv.reader(csv_file)
        # next(csv_reader)
        # for row in csv_reader:
        #     validation_type = row[0]
        #     source_conn = row[1]
        #     target_conn = row[2]
        #     source_table = row[3]
        #     target_table = row[4]
        #     primary_keys = row[5]
        #     column_validation_type = row[6]
        #     row_validation_type = row[7]
        #     exclude_columns = row[8]
        #     source_sql_location = row[9]
        #     target_sql_location = row[10]
        #     output_table = row[11]
    

    df = pd.read_csv('dvt_executions.csv')
    for index, row in df.iterrows():
        if row['validation_type'] == 'column':
            # print(row)
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
    