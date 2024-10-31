

import os
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.oauth2 import service_account
import db_dtypes

# Path of downloaded files
Server_path_Fire_Incidents  = r'C:/bigq/Fire_Incidents_20241025.csv'

import pandas as pd
import csv
client = bigquery.Client.from_service_account_json('C:/bigq/primal-cascade-324404-914ca5be4dde.json')

## The business intelligence team needs to run queries that aggregate these incidents
## along the following dimensions: time period, district, and battalion


query = """

"""



# Start
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)


df_Fire_Incidents  = pd.read_csv(Server_path_Fire_Incidents,low_memory=False,delimiter=',')


dataser_ref = client.dataset('TesteDados')
table_df_Fire_Incidents = dataser_ref.table('Fire_Incidents_20241025')
client.load_table_from_dataframe(df_Fire_Incidents,table_df_Fire_Incidents).result()

query_job = client.query(query).result()

dfff= list(query_job)

print(dfff)

ddf=pd.DataFrame(dfff)

print(ddf)
csv_string = ddf.to_csv(index=False)

#print(csv_string)



