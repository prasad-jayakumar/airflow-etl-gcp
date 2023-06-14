from airflow import DAG
import json
import csv
import io
import uuid
import pymongo
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from airflow.models import Variable
from serpapi import GoogleSearch
from io import BytesIO, StringIO
from pymongo import MongoClient, errors
from datetime import datetime, timedelta
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud.bigquery.schema import SchemaField
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook



default_args = {
    "owner": "prasad.jayakumar@sjsu.edu",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 4, 10, 0),
    # "start_date": days_ago(1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "extract-transform-load-to-bq",
    default_args=default_args,
    description="Extract, transform, and load a CSV file to BigQuery",
    schedule_interval='@daily',
    # schedule_interval=None
)


# Define the input and output file locations
input_bucket = 'raw_job_data'
input_file = 'gsearch_jobs.csv'
input_json_file='jobs.json'

staged_bucket = 'staged_job_data'
staged_file='staged_job_data.csv'
exception_file = 'exception_job_data.csv'
staged_table = 'database-etl.job_market_data.staged_table'
exception_table = 'database-etl.job_market_data.exception_table'

transformed_bucket = 'transformed_job_data'
transformed_file = 'transformed_gsearch_jobs.csv'
facts_and_dimensions_bucket='facts_dimensions'

company_dim_bucket_file='dim_company.csv'
company_dim_table='database-etl.job_market_data.dim_company'
via_dim_bucket_file='dim_via.csv'
via_dim_table='database-etl.job_market_data.dim_via'
city_dim_bucket_file='dim_city.csv'
city_dim_table='database-etl.job_market_data.dim_city'
state_dim_bucket_file='dim_state.csv'
state_dim_table='database-etl.job_market_data.dim_state'
fact_job_bucket_file='fact_job.csv'
fact_job_table='database-etl.job_market_data.fact_job'

# MongoDB connection details
MONGO_URI = Variable.get("MONGO_URI")
DB_NAME = "jobs-data"
COLLECTION_NAME = "jobs"
# Connect to MongoDB
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db = mongo_client[DB_NAME]
mongo_collection = mongo_db[COLLECTION_NAME]

storage_client= storage.Client()
bq_client = bigquery.Client()


###########     Utility method  ####################
# Define a function to strip whitespace from strings
def strip_strings(x):
    if isinstance(x, str):
        return x.strip()
    else:
        return x

def validate_data(df):
    ##removing unnamed column
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    ##dropping unnecessary columns
    formatted_df = df.drop(['thumbnail', 'index', 'salary', 'salary_pay', 'search_term', 'commute_time', 'search_location', 'salary_avg', 'salary_min', 'salary_max','salary_hourly', 'salary_yearly'], axis=1)
    ##renaming columns
    formatted_df.columns = ['title', 'company_name', 'location', 'via', 'description', 'extensions', 'job_id', 'posted_at', 'schedule_type', 'wfh', 'timestamp', 'salary_type', 'salary',  'skills']
    ## reindexing columns
    formatted_df = formatted_df.reindex(columns=['job_id', 'title', 'company_name', 'location', 'via', 'description', 'extensions', 'posted_at', 'schedule_type', 'wfh', 'timestamp', 'salary_type', 'salary', 'skills'])
    ## cleaing all the strings
    formatted_df = formatted_df.applymap(strip_strings)
    ## Check if job_id, company_name, title, index, and via columns have null values
    exception_df = formatted_df[formatted_df[['job_id', 'company_name', 'title', 'via', 'location']].isnull().any(axis=1)]
    ## staging formatted data
    staging_df = formatted_df.dropna(subset=['job_id', 'company_name', 'title', 'via', 'location'])
    return exception_df, staging_df

##################################################


###########################         Task 1      ############################
def get_api_data():
    params = {
      "api_key": Variable.get("SERP_API_KEY"),
      "engine": "google_jobs",
      "google_domain": "google.com",
      "q": "data analysts"
    }

    search = GoogleSearch(params)
    results = search.get_dict()
    print(len(results['jobs_results']))
    if results: 
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(bucket_name=input_bucket, object_name=input_json_file, data=json.dumps(results))
        return {'gcs_key': 'jobs.json'}
    else:
        print('API call failed with status code')



###########################     Task 2         #############################
def save_api_data_to_mongodb():
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    data = gcs_hook.download(bucket_name=input_bucket, object_name=input_json_file)
    data = json.loads(data)
    if data is not None:
        # Save each document to MongoDB with job ID as key
        for result in data['jobs_results']:
            job_id = result["job_id"]
            result['timestamp'] = datetime.now()
            del result["job_id"]
            update_result = mongo_collection.update_one({"_id": job_id}, {"$set": result}, upsert=True)
            if update_result.modified_count == 1 or update_result.upserted_id:
                # log successful update or upsert
                print(f'Successfully updated or upserted document with id {job_id}')
            else:
                # log unsuccessful update or upsert
                print(f'Failed to update or upsert document with id {job_id}')
    else:
        print("No data to save")



################################        Task 3         #########################

def stage_data_from_mongodb():
    # Query the collection and return the results
    data = mongo_collection.find()
    transformed_data = []
    # Transform the data
    for row in data:
        transformed_row = {
            'job_id': row['_id'].strip(),
            'title': row['title'].strip(),  
            'company_name': row['company_name'].strip(),
            'location': row['location'].strip(),
            'via': row['via'].strip(),
            'description': row['description'].strip(),
            'extensions': row['extensions'],
            'posted_at': row['detected_extensions']['posted_at'] if 'detected_extensions' in row and 'posted_at' in row['detected_extensions'] else '',
            'schedule_type': row['detected_extensions']['schedule_type'] if 'detected_extensions' in row and 'schedule_type' in row['detected_extensions'] else '',
            'wfh': '',
            'timestamp': row['timestamp'],
            'salary_type': '',
            'salary': '',
        }
        transformed_data.append(transformed_row)

    # Define the BigQuery table schema
    table_schema = [
        {"name": "job_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "company_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "location", "type": "STRING", "mode": "NULLABLE"},
        {"name": "via", "type": "STRING", "mode": "NULLABLE"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "extensions", "type": "STRING", "mode": "REPEATED"},
        {"name": "posted_at", "type": "STRING", "mode": "NULLABLE"},
        {"name": "schedule_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "wfh", "type": "STRING", "mode": "NULLABLE"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "salary_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "salary", "type": "FLOAT64", "mode": "NULLABLE"}
    ]

    # Write the transformed data to a CSV file in memory
    with io.StringIO() as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=[field['name'] for field in table_schema])
        writer.writeheader()
        for row in transformed_data:
            writer.writerow(row)
        csv_data = csv_file.getvalue().encode()

    ## converting api data to dataframe
    api_df = pd.read_csv(StringIO(csv_data.decode('utf-8')))
        
    ## get staged data
    bucket = storage_client.get_bucket(staged_bucket)
    blob = bucket.blob(staged_file)
    staged_df = pd.read_csv(StringIO(blob.download_as_string().decode('utf-8')))

    ## combining api data and staged data
    combined_df = pd.concat([api_df, staged_df])
    combined_df.drop_duplicates(subset=['job_id'])
    ## updating the combined data to staged file
    combined_df.to_csv(staged_file, index=False)
    with open(staged_file, 'rb') as f:
        blob.upload_from_file(f)


##############################      Task 4      ##################################

def load_staging_data_from_csv_separate_exceptions():
    # Get CSV file from GCS bucket
    bucket = storage_client.get_bucket(input_bucket)
    blob = bucket.blob(input_file)
    csv_data = blob.download_as_string().decode('utf-8')
    # Convert CSV data to DataFrame
    df = pd.read_csv(StringIO(csv_data))
    print('initial raw data load : ', df.shape)
    exception_df, staging_df =  validate_data(df)
    
    print('Exception data : ', exception_df.shape)
    print('Staging data : ', staging_df.shape)

    exception_df.to_csv(exception_file, index=False)
    bucket = storage_client.get_bucket(staged_bucket)
    exception_blob = bucket.blob(exception_file)
    with open(exception_file, 'rb') as f:
        exception_blob.upload_from_file(f)
   
    ## staging formatted data
    bucket = storage_client.get_bucket(staged_bucket)
    blob = bucket.blob(staged_file)
    staging_df.to_csv(staged_file, index=False)
    with open(staged_file, 'rb') as f:
        blob.upload_from_file(f)


################################        Task 5      #############################

def load_and_clean_staged_data():
    # Get CSV file from GCS bucket
    bucket = storage_client.get_bucket(staged_bucket)
    blob = bucket.blob(staged_file)
    csv_data = blob.download_as_string().decode('utf-8')

    # Convert CSV data to DataFrame
    df = pd.read_csv(StringIO(csv_data))
    
    print('initial staged data size : ', df.shape)

    formatted_df = df.drop_duplicates()
    
    ## cleaning via strings
    formatted_df['via'] = formatted_df['via'].str.replace('via', '')

    # Split the 'location' column into separate 'city' and 'state' columns
    formatted_df[['city', 'state']] = formatted_df['location'].str.split(', ', expand=True)
    formatted_df.drop(columns=['location'], inplace=True)
    formatted_df['city'] = formatted_df['city'].str.replace(r'(United States).*', r'\1', regex=True)
    formatted_df['city'].fillna('anywhere', inplace=True)
    formatted_df['state'].fillna('anywhere', inplace=True)

    salary_type_dict = {
        'a year' : 'yearly',
        'an hour' : 'hourly',
        'a month' : 'monthly'
    }

    formatted_df['salary_type'] = formatted_df['salary_type'].replace(salary_type_dict)

    ##setting lower cases for all dimensions
    formatted_df['company_name'] = formatted_df['company_name'].str.lower()
    formatted_df['via'] = formatted_df['via'].str.lower()
    formatted_df['city'] = formatted_df['city'].str.lower()
    formatted_df['state'] = formatted_df['state'].str.lower()

    bucket = storage_client.get_bucket(transformed_bucket)
    blob = bucket.blob(transformed_file)
    formatted_df.to_csv(transformed_file, index=False)
    with open(transformed_file, 'rb') as f:
        blob.upload_from_file(f)


###############################     Task 6      ###############################

def separate_into_facts_dimensions():
    # Get CSV file from GCS bucket
    bucket = storage_client.get_bucket(transformed_bucket)
    blob = bucket.blob(transformed_file)
    csv_data = blob.download_as_string().decode('utf-8')
    # Convert CSV data to DataFrame
    jobs_df = pd.read_csv(StringIO(csv_data))
    print('cleaned df columns - ', jobs_df.columns)
    print(jobs_df.shape)


    ############     Prepare the dataframe with new data   #############
    table_id = 'job_market_data.fact_job'
    query = """
        SELECT t.job_id
        FROM {} t
    """.format(table_id)
    result = bq_client.query(query).to_dataframe()
    print(result.shape)
    # Filter already existing rows based on job_id
    existing_job_df = jobs_df['job_id'].isin(result['job_id'])
    print('existing jobs, ', existing_job_df.shape)
    
    jobs_df = jobs_df[~existing_job_df]

    dim_company_df_list = []
    dim_via_df_list = []
    dim_state_df_list = []
    dim_city_df_list = []
    fact_job_df_list = []


    #################       Processing data in chunks       #######################

    for i, df in jobs_df.groupby(jobs_df.index // 500):

        print('#################  performing chunk no: ', i)

        ########################################         Separate dim_company       ###########################################
        # Create a temporary table with the DataFrame data

        table_id = 'job_market_data.temp_company_table'
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.schema = [
            bigquery.SchemaField('company_name', 'STRING')
        ]
        load_job = bq_client.load_table_from_dataframe(df[['company_name']], table_id, job_config=job_config)
        load_job.result()


        # Join the temporary table with the original table to get the IDs
        query = """
            SELECT t.company_name, o.id AS company_id
            FROM {} t
            LEFT JOIN `job_market_data.dim_company` o
            ON t.company_name = o.company_name
        """.format(table_id)
        result = bq_client.query(query).to_dataframe()
        print(result.shape)
        # Merge the result back into the DataFrame
        df = df.merge(result, on='company_name')
        df = df.drop_duplicates()

        ### Separate new company_name and add it to dimensions - dim_company
        print('creating company_dim_df table')
        company_dim_df = df.loc[df['company_id'].isnull()]
        company_dim_df = company_dim_df.drop_duplicates(subset=['company_name'])
        company_dim_df['id'] = company_dim_df['company_name'].apply(lambda x: uuid.uuid5(uuid.NAMESPACE_URL, str(x)))
        company_dim_df = company_dim_df[['id', 'company_name']]
        print('company_dim_df : ', company_dim_df.shape)
        
        dim_company_df_list.append(company_dim_df)

        ##### Processing fact dataframe
        print('before merging company_id', df.shape)
        df = pd.merge(df, company_dim_df, on='company_name', how='left')
        df['company_id'] = df['company_id'].fillna(df['id'])
        df.drop(columns=['id', 'company_name'], inplace=True)
        df = df.drop_duplicates()
        print('after merging company_id', df.shape)

        #########################################   end of dim_company    ##########################################


        ###############################         Separate new dim_via    ###############################
        print('creating dim_via table')

        ###### Fill up ids from dim_via
        # Create a temporary table with the DataFrame data
        table_id = 'job_market_data.temp_via_table'
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.schema = [
            bigquery.SchemaField('via', 'STRING')
        ]
        load_job = bq_client.load_table_from_dataframe(df[['via']], table_id, job_config=job_config)
        load_job.result()


        # Join the temporary table with the original table to get the IDs
        query = """
            SELECT t.via, o.id AS via_id
            FROM {} t
            LEFT JOIN `job_market_data.dim_via` o
            ON t.via = o.via
        """.format(table_id)
        result = bq_client.query(query).to_dataframe()
        print(result.shape)
        # Merge the result back into the DataFrame
        df = df.merge(result, on='via')
        df = df.drop_duplicates()

        ### Separate new via and add it to dimensions - dim_via
        print('creating via_dim_df table')
        via_dim_df = df.loc[df['via_id'].isnull()]
        via_dim_df = via_dim_df.drop_duplicates(subset=['via'])
        via_dim_df['id'] = via_dim_df['via'].apply(lambda x: uuid.uuid5(uuid.NAMESPACE_URL, str(x)))
        via_dim_df = via_dim_df[['id', 'via']]
        print('via_dim_df : ', via_dim_df.shape)
        
        dim_via_df_list.append(via_dim_df)

        ###### Processing fact dataframe
        print('before merging via_id', df.shape)
        df = pd.merge(df, via_dim_df, on='via', how='left')
        df['via_id'] = df['via_id'].fillna(df['id'])
        df.drop(columns=['id', 'via'], inplace=True)
        df = df.drop_duplicates()
        print('after merging via_id', df.shape)


        ###########################################     end of dim_via      #############################################


        ###############################         Separate new dim_state    ###############################
        print('creating dim_state table')
        ###### Fill up ids from dim_state
        # Create a temporary table with the DataFrame data
        table_id = 'job_market_data.temp_state_table'
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.schema = [
            bigquery.SchemaField('state', 'STRING')
        ]
        load_job = bq_client.load_table_from_dataframe(df[['state']], table_id, job_config=job_config)
        load_job.result()


        # Join the temporary table with the original table to get the IDs
        query = """
            SELECT t.state, o.id AS state_id
            FROM {} t
            LEFT JOIN `job_market_data.dim_state` o
            ON t.state = o.state
        """.format(table_id)
        result = bq_client.query(query).to_dataframe()
        print(result.shape)
        # Merge the result back into the DataFrame
        df = df.merge(result, on='state')
        df = df.drop_duplicates()

        ### Separate new state and add it to dimensions - dim_state
        print('creating dim_state table')
        state_dim_df = df.loc[df['state_id'].isnull()]
        state_dim_df = state_dim_df.drop_duplicates(subset=['state'])
        state_dim_df['id'] = state_dim_df['state'].apply(lambda x: uuid.uuid5(uuid.NAMESPACE_URL, str(x)))
        state_dim_df = state_dim_df[['id', 'state']]
        print('state_dim_df : ', state_dim_df.shape)
        
        dim_state_df_list.append(state_dim_df)

        ###### Processing fact dataframe
        print('before merging state_id', df.shape)
        df = pd.merge(df, state_dim_df, on='state', how='left')
        df['state_id'] = df['state_id'].fillna(df['id'])
        df.drop(columns=['id', 'state'], inplace=True)
        df = df.drop_duplicates()
        print('after merging state_id', df.shape)


        ###########################################     end of dim_state      #############################################

        

        ###############################         Separate new dim_city   ###############################
        
        print('creating dim_city table')

        # Create a temporary table with the DataFrame data
        table_city_id = 'job_market_data.temp_city_table'
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.schema = [
            bigquery.SchemaField('city', 'STRING')
        ]
        load_job = bq_client.load_table_from_dataframe(df[['city']], table_city_id, job_config=job_config)
        load_job.result()


        # Join the temporary table with the original table to get the IDs
        query = """
            SELECT t.city, o.id AS city_id
            FROM {} t
            LEFT JOIN `job_market_data.dim_city` o
            ON t.city = o.city
        """.format(table_city_id)
        result = bq_client.query(query).to_dataframe()
        print(result.shape)
        # Merge the result back into the DataFrame
        df = df.merge(result, on='city')
        df = df.drop_duplicates()

        ### Separate new city and add it to dimensions - dim_city
        print('creating dim_city table')
        city_dim_df = df.loc[df['city_id'].isnull()]
        city_dim_df = city_dim_df.drop_duplicates(subset=['city'])
        city_dim_df['id'] = city_dim_df['city'].apply(lambda x: uuid.uuid5(uuid.NAMESPACE_URL, str(x)))
        city_dim_df = city_dim_df[['id', 'city']]
        print('city_dim_df : ', city_dim_df.shape)
        
        dim_city_df_list.append(city_dim_df)

        ###### Processing fact dataframe
        print('before merging city_id', df.shape)
        df = pd.merge(df, city_dim_df, on='city', how='left')
        df['city_id'] = df['city_id'].fillna(df['id'])
        df.drop(columns=['id', 'city'], inplace=True)
        df = df.drop_duplicates()
        print('after merging city_id', df.shape)


        ###########################################     end of dim_city      #############################################


        ########################################     fact table - job_table    #############################################
        print('fact table shape', df.shape)
        df = df.reindex(columns=['job_id', 'company_id', 'via_id', 'city_id', 'state_id', 'title', 'description', 'extensions', 'posted_at', 'schedule_type', 'wfh', 'timestamp', 'salary_type', 'salary', 'skills'])

        fact_job_df_list.append(df)
        
        ############## end of chunk loop   ##################


    tasks = []
    ### preparing the final dfs 
    if dim_company_df_list:
        company_dim_df_final = pd.concat(dim_company_df_list)
        print(company_dim_df_final.columns)
        company_dim_df_final = company_dim_df_final.drop_duplicates(subset=['id'])
        print('final company dim shape : ', company_dim_df_final.shape)
        bucket = storage_client.get_bucket(facts_and_dimensions_bucket)
        blob = bucket.blob(company_dim_bucket_file)
        company_dim_df_final.to_csv(company_dim_bucket_file, index=False)
        with open(company_dim_bucket_file, 'rb') as f:
            blob.upload_from_file(f)
        tasks.append('load_dim_company_to_bigquery')

    if dim_via_df_list:
        via_dim_df_final = pd.concat(dim_via_df_list)
        via_dim_df_final = via_dim_df_final.drop_duplicates(subset=['id'])
        print('final via dim shape : ', via_dim_df_final.shape)
        bucket = storage_client.get_bucket(facts_and_dimensions_bucket)
        blob = bucket.blob(via_dim_bucket_file)
        via_dim_df_final.to_csv(via_dim_bucket_file, index=False)
        with open(via_dim_bucket_file, 'rb') as f:
            blob.upload_from_file(f)
        tasks.append('load_dim_via_to_bigquery')

    if dim_state_df_list:
        state_dim_df_final = pd.concat(dim_state_df_list)
        state_dim_df_final = state_dim_df_final.drop_duplicates(subset=['id'])
        print('final state dim shape : ', state_dim_df_final.shape)
        bucket = storage_client.get_bucket(facts_and_dimensions_bucket)
        blob = bucket.blob(state_dim_bucket_file)
        state_dim_df_final.to_csv(state_dim_bucket_file, index=False)
        with open(state_dim_bucket_file, 'rb') as f:
            blob.upload_from_file(f)
        tasks.append('load_dim_city_to_bigquery')        

    if dim_city_df_list:
        city_dim_df_final = pd.concat(dim_city_df_list)
        city_dim_df_final = city_dim_df_final.drop_duplicates(subset=['id'])
        print('final city dim shape : ', city_dim_df_final.shape)
        bucket = storage_client.get_bucket(facts_and_dimensions_bucket)
        blob = bucket.blob(city_dim_bucket_file)
        city_dim_df_final.to_csv(city_dim_bucket_file, index=False)
        with open(city_dim_bucket_file, 'rb') as f:
            blob.upload_from_file(f)
        tasks.append('load_dim_state_to_bigquery')        

    if fact_job_df_list:
        fact_job_df_final = pd.concat(fact_job_df_list)
        fact_job_df_final = fact_job_df_final.drop_duplicates(subset=['job_id'])
        print('final fact job dim shape : ', fact_job_df_final.shape)
        bucket = storage_client.get_bucket(facts_and_dimensions_bucket)
        blob = bucket.blob(fact_job_bucket_file)
        fact_job_df_final.to_csv(fact_job_bucket_file, index=False)
        with open(fact_job_bucket_file, 'rb') as f:
            blob.upload_from_file(f)
        tasks.append('load_fact_job_to_bigquery')        
    return tasks


############################        Loading data to big query tasks     ########################

def load_dim_company_to_bigquery():
    load_job = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_company_to_bigquery',
        bucket=facts_and_dimensions_bucket,
        source_objects=[company_dim_bucket_file],
        destination_project_dataset_table=company_dim_table,
        schema_fields=[
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "company_name", "type": "STRING", "mode": "NULLABLE"}

        ],
        skip_leading_rows=1,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_APPEND",
        # write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        dag=dag,
    )
    return load_job


def load_dim_via_to_bigquery():
    load_job = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_via_to_bigquery',
        bucket=facts_and_dimensions_bucket,
        source_objects=[via_dim_bucket_file],
        destination_project_dataset_table=via_dim_table,
        schema_fields=[
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "via", "type": "STRING", "mode": "NULLABLE"}

        ],
        skip_leading_rows=1,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_APPEND",
        # write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        dag=dag,
    )
    return load_job


def load_dim_city_to_bigquery():
    load_job = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_city_to_bigquery',
        bucket=facts_and_dimensions_bucket,
        source_objects=[city_dim_bucket_file],
        destination_project_dataset_table=city_dim_table,
        schema_fields=[
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"}

        ],
        skip_leading_rows=1,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_APPEND",
        # write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        dag=dag,
    )
    return load_job


def load_dim_state_to_bigquery():
    load_job = GoogleCloudStorageToBigQueryOperator(
        task_id='load_dim_state_to_bigquery',
        bucket=facts_and_dimensions_bucket,
        source_objects=[state_dim_bucket_file],
        destination_project_dataset_table=state_dim_table,
        schema_fields=[
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"}

        ],
        skip_leading_rows=1,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_APPEND",
        # write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        dag=dag,
    )
    return load_job


def load_staging_to_bigquery():
    load_job = GoogleCloudStorageToBigQueryOperator(
        task_id='load_staging_to_bigquery',
        bucket=staged_bucket,
        source_objects=[staged_file],
        destination_project_dataset_table=staged_table,
        schema_fields=[
            {"name": "job_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "company_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "via", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "extensions", "type": "STRING", "mode": "NULLABLE"},
            {"name": "posted_at", "type": "STRING", "mode": "NULLABLE"},
            {"name": "schedule_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "wfh", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "salary_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "salary", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "skills", "type": "STRING", "mode": "NULLABLE"}
        ],
        skip_leading_rows=1,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        # write_disposition="WRITE_APPEND",
        write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        dag=dag,
    )
    return load_job

def load_exception_to_bigquery():
    load_job = GoogleCloudStorageToBigQueryOperator(
        task_id='load_exception_to_bigquery',
        bucket=staged_bucket,
        source_objects=[exception_file],
        destination_project_dataset_table=exception_table,
        schema_fields=[
            {"name": "job_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "company_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "via", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "extensions", "type": "STRING", "mode": "NULLABLE"},
            {"name": "posted_at", "type": "STRING", "mode": "NULLABLE"},
            {"name": "schedule_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "wfh", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "salary_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "salary", "type": "STRING", "mode": "NULLABLE"},
            {"name": "skills", "type": "STRING", "mode": "NULLABLE"}
        ],
        skip_leading_rows=1,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_APPEND",
        # write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        dag=dag,
    )
    return load_job

def load_fact_job_to_bigquery():
    load_job = GoogleCloudStorageToBigQueryOperator(
        task_id='load_fact_job_to_bigquery',
        bucket=facts_and_dimensions_bucket,
        source_objects=[fact_job_bucket_file],
        destination_project_dataset_table=fact_job_table,
        schema_fields=[
            {"name": "job_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "company_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "via_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "extensions", "type": "STRING", "mode": "NULLABLE"},
            {"name": "posted_at", "type": "STRING", "mode": "NULLABLE"},
            {"name": "schedule_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "wfh", "type": "STRING", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "salary_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "salary", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "skills", "type": "STRING", "mode": "NULLABLE"}

        ],
        skip_leading_rows=1,
        source_format="CSV",
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_APPEND",
        # write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        dag=dag,
    )
    return load_job

##########################################################################################################

load_data_from_api_task = PythonOperator(
    task_id='load_data_from_api_task',
    python_callable=get_api_data,
    dag=dag
)

save_api_data_to_mongodb_task = PythonOperator(
    task_id='save_api_data_to_mongodb',
    python_callable=save_api_data_to_mongodb,
    dag=dag
)

staging_layer = PythonOperator(
    task_id='staging_layer',
    python_callable=stage_data_from_mongodb,
    dag=dag
)

load_data_from_csv_task = PythonOperator(
    task_id='load_data_from_csv_task',
    python_callable=load_staging_data_from_csv_separate_exceptions,
    dag=dag,
)

load_and_clean_staged_data_task = PythonOperator(
    task_id='load_and_clean_staged_data',
    python_callable=load_and_clean_staged_data,
    dag=dag,
)

separate_into_facts_dimensions_task = BranchPythonOperator(
    task_id='separate_into_facts_dimensions',
    python_callable=separate_into_facts_dimensions,
    dag=dag,
)

load_staged_data_to_bigquery = load_staging_to_bigquery()
load_exception_data_to_bigquery = load_exception_to_bigquery()
load_dim_company_to_bigquery_task = load_dim_company_to_bigquery()
load_dim_via_to_bigquery_task = load_dim_via_to_bigquery()
load_dim_city_to_bigquery_task = load_dim_city_to_bigquery()
load_dim_state_to_bigquery_task = load_dim_state_to_bigquery()
load_fact_job_to_bigquery_task = load_fact_job_to_bigquery()

[load_data_from_csv_task, load_data_from_api_task] >> save_api_data_to_mongodb_task >> staging_layer >> [load_staged_data_to_bigquery, load_exception_data_to_bigquery] >> load_and_clean_staged_data_task >>  separate_into_facts_dimensions_task >> [load_dim_company_to_bigquery_task, load_dim_via_to_bigquery_task, load_dim_city_to_bigquery_task, load_dim_state_to_bigquery_task, load_fact_job_to_bigquery_task]
