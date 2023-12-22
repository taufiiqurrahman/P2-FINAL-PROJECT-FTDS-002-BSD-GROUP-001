'''
==================================================================================================
Final Project (Dec 8 2023)

Batch   : BSD-002
Group   : 1
Members : Audrey Wanto
          Jeni Kasturi
          Taufiqurrahman

Objective : This program is made to visualize the Directed Acyclic Graph (DAG) using Apache
            Airflow. As context with Data Engineering, the DAG will process the data extracted
            from the SQL database, in which the data will then be loaded and transformed.
            Using Airflow, the data will go through automatic processing if there are any 
            new data updated within the database.
==================================================================================================
'''

# Import Libraries
import pandas as pd
import numpy as np
import psycopg2 as db
import datetime as dt
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator

# Method definitions
def fetchingPostgresql():
    '''
    This method takes the data from PostgreSQL which will then be continued for Data Cleaning
    
    Parameters:
    url: string - PostgreSQL location
    database: string - Name of the database of where the data was saved
    table: string - Name of the table where the data was saved
    
    Return (NONE)
    
    Example usage: 
    fetchData = PythonOperator(task_id='fetch', python_callable=fetchingPostgresql)
    '''
    
    # Initializing variables
    db_name = 'airflow'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'localhost'
    db_port = '5432'
    
    # Connection with Postgre
    connection = db.connect(
        database=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    
    # Query to extract data from table and save into dataframe
    customer_info = pd.read_sql("SELECT * FROM customer_info", connection)
    customer_cases = pd.read_sql("SELECT * FROM customer_cases", connection)
    customer_product = pd.read_sql("SELECT * FROM customer_product", connection)
    product_info = pd.read_sql("SELECT * FROM product_info", connection)
    
    # Save query into CSV files
    customer_info.to_csv('/opt/airflow/dags/customer_info.csv', index=False)
    customer_cases.to_csv('/opt/airflow/dags/customer_cases.csv', index=False)
    customer_product.to_csv('/opt/airflow/dags/customer_product.csv', index=False)
    product_info.to_csv('/opt/airflow/dags/product_info.csv', index=False)    

def mergingDataframe():
    '''
    This method will utilize all the dataframes that were previously extracted to merge into one Dataframe
    
    Parameters (NONE)
    
    Return (NONE)
    
    Example usage:
    mergeData = PythonOperator(task_id='merge', python_callable=mergingDataframe)
    '''
    # Reading csv into dataframes
    df1 = pd.read_csv('/opt/airflow/dags/customer_info.csv')
    df2 = pd.read_csv('/opt/airflow/dags/customer_cases.csv')
    df3 = pd.read_csv('/opt/airflow/dags/customer_product.csv')
    df4 = pd.read_csv('/opt/airflow/dags/product_info.csv')
    
    # Drop columns before merging
    df1 = df1.drop(columns=['Unnamed: 0'])
    df2 = df2.drop(columns=['Unnamed: 0'])
    df3 = df3.drop(columns=['Unnamed: 0'])
    
    # Rename column
    df3 = df3.rename(columns={'product': 'product_id'})
    
    # Merge dataframes into one dataframe
    merged_df = pd.merge(df1, df2, on='customer_id', how='inner')
    merged_df = pd.merge(merged_df, df3, on='customer_id', how='inner')
    merged_df = pd.merge(merged_df, df4, on='product_id', how='inner')
    
    # Drop duplicates and reset index
    df = merged_df.drop_duplicates()
    df = merged_df.reset_index(drop=True)
    
    # Save merged dataframe into CSV file
    df.to_csv('/opt/airflow/dags/merged_data.csv', index=False)
    
def cleaningData():
    '''
    This function is used to clean the merged dataframe's data, along with adding columns to calculate revenue and churn
    
    Parameters (NONE)
    
    Return (NONE)
    
    Example usage:
    cleanData = PythonOperator(task_id='clean', python_callable=cleaningData)
    '''
    # Read CSV into dataframe
    df = pd.read_csv('/opt/airflow/dags/merged_data.csv')
    
    # Create a column for churn
    churn_list = []
    for cust in df['cancel_date_time']:
        if type(cust) == float:
            churn_list.append(0)
        else:
            churn_list.append(1)
    df['churn'] = churn_list
    
    # Change data type of signup_date_time and cancel_date_time
    df['signup_date_time'] = pd.to_datetime(df['signup_date_time'])
    df['cancel_date_time'] = pd.to_datetime(df['cancel_date_time'])
    
    # Take year and month of signup_date_time and cancel_date_time
    df['signup_month'] = df['signup_date_time'].dt.month
    df['signup_year'] = df['signup_date_time'].dt.year
    df['signup_day'] = df['signup_date_time'].dt.day
    df['cancel_month'] = df['cancel_date_time'].dt.month
    df['cancel_year'] = df['cancel_date_time'].dt.year
    
    # Create new column for total revenue so far for each customer
    revenue_list = []
    revenue = 0
    duration = 0
    max = df['cancel_date_time'].max() # Data stops at the limit
    for index, row in df.iterrows():
        if row['name'] == 'annual_subscription' and row['churn'] == 1:
            duration = row['cancel_year'] - row['signup_year']
            revenue = duration * 1200
            revenue_list.append(revenue)
        elif row['name'] == 'annual_subscription' and row['churn'] == 0:
            duration = 2021 - row['signup_year'] # Data stops at 2021, hence this will be the limit
            revenue = duration * 1200
            revenue_list.append(revenue)
        elif row['name'] == 'monthly_subscription' and row['churn'] == 1:
            duration = ((row['cancel_date_time'] - row['signup_date_time'])//np.timedelta64(1, 'M'))
            revenue = duration * 125
            revenue_list.append(revenue)
        elif row['name'] == 'monthly_subscription' and row['churn'] == 0:
            duration = ((max - row['signup_date_time'])//np.timedelta64(1, 'M'))
            revenue = duration * 125
            revenue_list.append(revenue)
    df['revenue'] = revenue_list
    
    # Save cleaned data into a new CSV file
    df.to_csv('/opt/airflow/dags/cleaned_data.csv')
    
def toScientistAnalyst():
    '''
    This method sends the data to the Data Scientist and Data Analyst in the form of CSV file
    
    Parameters (NONE)

    Return (NONE)

    Example usage:
    sendData = PythonOperator(task_id='sendData', python_callable=toScientistAnalyst)
    '''
    # Read CSV file into df
    df = pd.read_csv('opt/airflow/dags/cleaned_data.csv')
    df
    
default_args = {
    'owner': 'Group 1 - BSD',
    'start_date': dt.datetime(2023, 12, 3),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=60),
}

with DAG('FinalProject',
         default_args=default_args,
         schedule_interval=timedelta(minutes=60),      
         ) as dag:

    fetchData = PythonOperator(task_id='fetch',
                                 python_callable=fetchingPostgresql)
    
    mergeData = PythonOperator(task_id='merge',
                                 python_callable=mergingDataframe)

    cleanData = PythonOperator(task_id='clean',
                                 python_callable=cleaningData)
    
    sendData = PythonOperator(task_id='send',
                                 python_callable=toScientistAnalyst)


fetchData >> mergeData >> cleanData >> sendData