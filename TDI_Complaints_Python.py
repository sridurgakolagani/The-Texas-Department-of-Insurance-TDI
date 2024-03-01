# The Texas Department of Insurance (TDI) handles complaints against people and organizations licensed by TDI, such as companies, agents, and adjusters.
# Extracting Insurance Complaints Data The API fetches the data in JSON format.

import pandas as pd
import requests
import warnings

# Suppress InsecureRequestWarning
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# API URL
url = "https://data.texas.gov/resource/ubdr-4uff.json"

# Parameters for pagination
offset = 0  # Start from the first page
limit = 1000  # Number of records per page
all_data = []

# Loop until all data is fetched
while True:
    # Send a GET request to the API URL with pagination parameters
    response = requests.get(url, params={"$limit": limit, "$offset": offset}, verify=False)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Convert the response to JSON format
        data = response.json()

        # If no more data is returned, break the loop
        if not data:
            break

        # Append fetched results into the list of all data
        all_data.extend(data)

        # Move to the next page
        offset += limit
    else:
        print(f"Failed to retrieve data from the API. Status code: {response.status_code}")
        break

# Create a DataFrame from the list of all data
df_insurance_complaint = pd.DataFrame(all_data)

# Display the DataFrame
print(df_insurance_complaint.head())



''''Transformation and Validation ''''

#fetching all rows and columns 
df_insurance_complaint.shape

#look at all the column names
print(df_insurance_complaint.columns.tolist())

#Display Index, Columns, and Data
df_insurance_complaint.info()


#Complaint_number
#We’ll check if the Complaint_number column has [i] no empty and/or null values, [ii] all values are numeric, 
# return indices for empty entries/values
df_insurance_complaint[df_insurance_complaint['complaint_number'] == ''].index
# check for NULL values
import numpy as np
np.where(pd.isnull(df_insurance_complaint['complaint_number']))
# check if all the values are numeric
pd.to_numeric(df_insurance_complaint['complaint_number'], errors='coerce').notnull().all()


#Respondent_id
#We’ll check if the respondent_id column has [i] no empty and/or null values, [ii] all values are numeric, 
# return indices for empty entries/values
df_insurance_complaint[df_insurance_complaint['respondent_id'] == ''].index
# check for NULL values
import numpy as np
np.where(pd.isnull(df_insurance_complaint['respondent_id']))
# check if all the values are numeric
pd.to_numeric(df_insurance_complaint['respondent_id'], errors='coerce').notnull().all()

#RECEIVED_DATE
#Let’s see if the column received_date has [i] no null values, and [ii] all values are datetime.
# check for NULL values
import numpy as np
np.where(pd.isnull(df_insurance_complaint['received_date']))
# check if all values are datetime
df_insurance_complaint['received_date'].astype(str).apply(lambda x: pd.to_datetime(x, errors='coerce')).notna().all()


#CLOSED_DATE
#Let’s see if the column closed_date has [i] no null values, and [ii] all values are datetime.
# check for NULL values
import numpy as np
np.where(pd.isnull(df_insurance_complaint['closed_date']))
# check if all values are datetime
df_insurance_complaint['closed_date'].astype(str).apply(lambda x: pd.to_datetime(x, errors='coerce')).notna().all()


# find unique values
df_insurance_complaint['respondent_type'].unique()
df_insurance_complaint['complaint_confirmed_code'].unique()
df_insurance_complaint['coverage_type'].unique()



---- Loading into Snowflake ----

# install Snowflake connector
pip install snowflake-connector-python

import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector.pandas_tools import write_pandas

# create a function to upload the data into a table in Snowflake
def upload_data_to_snowflake(ACCOUNT, USER, PASSWORD, WAREHOUSE, DATABASE, SCHEMA):
    
    # connect to Snowflake
    conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )
    
    # create a cursor
    cur = conn.cursor()
    
    # create the warehouse
    cur.execute(f'CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 300')
    
    # use the warehouse
    cur.execute(f'USE WAREHOUSE {WAREHOUSE}')
    
    # create the database
    cur.execute(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
    
    # use the database
    cur.execute(f'USE DATABASE {DATABASE}')
    
    # create the schema
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')
    
    # use the schema
    cur.execute(f'USE SCHEMA {SCHEMA}')
    
    # create the table
    cur.execute("""
    CREATE OR REPLACE TABLE Insurance_complaints (
        "complaint_number" INTEGER,
        "respondent_name" STRING,
        "complainant_role" STRING,
        "reason" STRING,
        "complaint_confirmed_code" STRING,
        "disposition" STRING,
        "received_date" TIMESTAMP,
        "closed_date" TIMESTAMP,
        "complaint_type" STRING,
        "coverage_type" STRING,
        "coverage_level" STRING,
        "involved_party_type" STRING,
        "respondent_id" INTEGER,
        "respondent_role" STRING,
        "respondent_type" STRING,
        "complainant_type" STRING,
        "keyword" STRING
        ) 
    """)
    
    # load the data from 'df_insurance_complaint' dataframe into 'Insurance_complaintS Snowflake table
    
    cur.execute('TRUNCATE TABLE Insurance_complaints')  # clear existing data if needed
    
    write_pandas(conn, df_insurance_complaint, 'Insurance_complaints')
    
    
    # close the cursor and Snowflake connection
    cur.close()
    conn.close()
    
# call the function
upload_to_snowflake('i*****c-y*****6', 'sridurgakolagani', '**********', 'COMPUTE_WH',' TexasDepartmentofInsurance', 'InsurancecomplaintsRawData')
