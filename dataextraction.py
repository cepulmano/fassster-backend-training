# Importing required modules
from datetime import datetime, timedelta
from google.cloud import bigquery

from decouple import config
import pandas as pd
import numpy as np
import os

if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=config('BQ_SERVICE_KEY_FILE')
    client = bigquery.Client()
    query_job = client.query("""SELECT * FROM `doh-covid-dwh.fassster_90_shared.linelist` WHERE Report_Date LIKE '%2023-%' """)
    results = query_job.result()  # Waits for job to complete.
    df = results.to_dataframe()

    df.to_parquet('df.linelist.gzip',compression='gzip')
    print(len(pd.read_parquet('df.linelist.gzip')))