from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

key_path = './conf_files/service-account-file.json'

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)


client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

q2 =(
"""
SELECT fullVisitorId, hits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
"""
)

query_job = client.query(q2)  # API request
rows = query_job.result()  # Waits for query to finish    

df_big = pd.DataFrame()

for row in rows:

    data = []
    data1 = []
    
    data.append(row[0])
    data1.append(row[1])
    
    df = pd.DataFrame(data1[0])
    df['fullVisitorId'] = data[0]
    
    df_big = df_big.append(df)
    
def get_transaction_id(x):
    try:
        return x.get('transactionId')
    except:
        return None
    
def get_transaction_revenue(x):
    try:
        return x.get('transactionRevenue')
    except:
        return None
        
df_big['transactionId'] = df_big['transaction'].apply(get_transaction_id)
df_big['transactionRevenue'] = df_big['transaction'].apply(get_transaction_revenue)

df_first = df_big[df_big['hitNumber'] == 1]
df_tran = df_big[~df_big['transactionRevenue'].isna()]
df_tran_final = df_tran.sort_values('hitNumber').groupby('fullVisitorId').apply(pd.DataFrame.head, n=1).reset_index(drop=True)

dff = df_first[['fullVisitorId', 'hour', 'minute']]
dtf = df_tran_final[['fullVisitorId', 'hour', 'minute']]
dtf = dtf.rename(columns={'hour': 'hour_converted', 'minute': 'minute_converted'})

final = pd.merge(dff, dtf, how='left', left_on = 'fullVisitorId', right_on='fullVisitorId')

print(final)