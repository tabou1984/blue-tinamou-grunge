from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
import pandas_gbq

from ef_assist_functions import *
from ef_queries import *

key_path = './conf_files/service-account-file.json'

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

#Function for exercise one. All the calculations and transformations are written in Python.
#The initial SQL query retrieves only top level columns (all the dictionaries are flattened
#inside the function)
#The only parameter is date (in 'yyyymmdd' form).

def exercice_one(date):
    query = (query_q1.format(date))
    df_init = pd.DataFrame()
    
    #Categories and subcategories (dictionaries and keys) that are required for the calculations
    categories = ['fullVisitorId', 'totals', 'device']
    tupled_categories = list((i, categories[i]) for i in range(len(categories)))
    cat_subcat_list = [('totals', 'visits'), ('totals', 'transactions'), ('device', 'deviceCategory')]
    
    #API call
    
    query_job = client.query(query)  # API request
    rows = query_job.result()  # Waits for query to finish 
    
    for row in rows:
        df = row_to_df(row, tupled_categories)
        df_init = df_init.append(df)
        
    for category, subcategory in cat_subcat_list:
        df_init[subcategory] = df_init[category].apply(master_subcategory_function(subcategory))
        
        
    #Transformations
    
    df_full = df_init[['fullVisitorId', 'visits', 'transactions', 'deviceCategory']]
    
    df_full['UserType'] = df_full.groupby('fullVisitorId')['fullVisitorId'].transform('size') > 1
    df_full['UserType'] = df_full['UserType'].apply(lambda x: 'Returning' if x == True else 'New')
    df_full['Platform'] = df_full['deviceCategory'].apply(lambda x: 'Web' if x == 'desktop' else 'Mobile')
    
    df_full = df_full[['fullVisitorId', 'visits', 'transactions', 'UserType', 'Platform']]
    
    #Calculations
    
    df_final = df_full.groupby(['UserType', 'Platform'])['visits', 'transactions'].sum().reset_index()
    df_final = df_final.append(df_final.sum(numeric_only=True), ignore_index=True)
    df_final['UserType'] = df_final['UserType'].fillna('Total')
    df_final['Platform'] = df_final['Platform'].fillna('Total')
    
    df_final['conversion_rate'] = df_final['transactions']/df_final['visits']
    
    return df_final

#Query for second exercise. In this section, all the work is happening in SQL. We just
#run the query and retrieve the results in pandas dataframe using pandas-gbq library
#The parameter is date (in 'yyyymmdd' form)

def exercice_two(date):
    query =(query_q2.format(date,date,date,date))
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = credentials.project_id
    
    df = pandas_gbq.read_gbq(query)
    
    return df