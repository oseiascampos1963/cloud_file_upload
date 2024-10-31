import base64
import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

# Define variables for testing the files uploads for Cloud Functions
bucket_name = 'my_1963'
project_name = 'primal-cascade-324404'
dataset_name = 'TesteDados'
table_name = 'Fire_Incidents_20241025'
file_name = 'Fire_Incidents_20241025.csv'

def generate_data():
    # Generate data
    
    # Get the current time
    
    #today = datetime.datetime.now().strftime('%Y-%m-%d')

    # Upload CSV file to Cloud Storage
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
   
    # Upload the CSV file from Cloud Storage to BigQuery
    client = bigquery.Client()
    table_id = project_name + '.' + dataset_name + '.' + table_name
    job_config = bigquery.LoadJobConfig(
      autodetect=True,
      max_bad_records = 100 ,
      source_format=bigquery.SourceFormat.CSV,
      write_disposition='WRITE_TRUNCATE'
    )
  
    uri = f'gs://gcf-v2-sources-1057837211035-southamerica-east1/Fire_Incidents_20241025.csv'
    load_job = client.load_table_from_uri(
      uri, table_id, job_config=job_config
    )  
    load_job.result()  



    # Here we aggregate these incidents to team using GROUP BY `Incident Date`,`neighborhood_district`,`Battalion`
    # The business intelligence team needs to run queries that aggregate these incidents
    # along the following dimensions: time period, district, and battalion
    # we have a memory limit for free cloud users so need to setup 2000 rows only

    query = """
    SELECT `Incident Date`,`neighborhood_district`,`Battalion`, COUNT(*) as NumberOfIncidents 
    FROM `primal-cascade-324404.TesteDados.Fire_Incidents_20241025` 
    GROUP BY `Incident Date`,`neighborhood_district`,`Battalion` limit 2000

    """

    # Here a quick convertion to make the file report look better

    query_job = client.query(query).result()
    #big_list= list(query_job)
    #dfreport = pd.DataFrame(big_list)
    totalList= list(query_job)

    incidentsDate=[]
    neighborhood_distrs=[]
    Battalions=[]
    NumberOfIncidents=[]

    for row in range(len(totalList)):
      incidentsDate.append(totalList[row]['Incident Date'])
      neighborhood_distrs.append(totalList[row]['neighborhood_district'])
      Battalions.append(totalList[row]['Battalion'])
      NumberOfIncidents.append(totalList[row]['NumberOfIncidents'])


    dict = {
    'Incident Dates' : incidentsDate,
    'District' : neighborhood_distrs,
    'Battalion ' : Battalions,
    'Number Of Incidents' : NumberOfIncidents
    }

    # Daily generation of the file 
    df_totalList = pd.DataFrame(dict)
    csv_report = df_totalList.to_csv(index=False)
    blob = bucket.blob(f'Incidents_report.csv')
    blob.upload_from_string(csv_report)


    # Make an API request and display prior a number of loaded rows to destination table
    destination_table = client.get_table(table_id) 
    print("Loaded {} rows.".format(destination_table.num_rows))

def hello_pubsub(event, context):
    """
    No valid Args passed, just mock data for testing upload file
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    generate_data()

if __name__ == "__main__":
    hello_pubsub('data', 'context')

