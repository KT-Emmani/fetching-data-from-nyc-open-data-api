#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata
from google.cloud import bigquery

# FETCH DATA (2020 - 2026) FROM SOCRATA API (NYC Open Data).
# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
# client = Socrata("data.cityofnewyork.us", None)

# Example authenticated client (needed for non-public datasets):
client = Socrata("data.cityofnewyork.us",
                 "YbXuI8uPOEUAGWyYV0WDnGFge",
                 username="etwumasi6913@gmail.com",
                 password="KwakuT1232468@")

# First 10000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results_2020_2026 = client.get("h9gi-nx95", limit = 10000)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results_2020_2026)

# INITIALIZE BIGQUERY CLIENT AND DEFINE DATASET ID 
bq_client = bigquery.Client()
table_id = "nyc-motor-vehicle-collision-12.nyc_motor_vehicle_collision_dataset.nyc_motor_vehicle_collision_table"

# LOAD RAW DATA INTO BIGQUERY (WAREHOUSE)
job = bq_client.load_table_from_dataframe(results_df, table_id)
job.result()  # Wait for the job to complete.   

print("Raw data loaded into BigQuery successfully.")

