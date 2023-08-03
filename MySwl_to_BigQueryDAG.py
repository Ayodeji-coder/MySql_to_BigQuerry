from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

# Default DAG arguments
default_args = {
    'owner': 'Ayodeji',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Full load process DAG
full_load_dag = DAG(
    'employees_full_load',
    default_args=default_args,
    description='Full load data from Employees table to BigQuery',
    schedule_interval=None,  
    start_date=datetime(2023, 8, 2),  
)

# Incremental load process DAG
incremental_dag = DAG(
    'employees_incremental',
    default_args=default_args,
    description='Incremental load data from Employees table to BigQuery',
    schedule_interval=timedelta(days=1),  # Daily schedule
    start_date=datetime(2023, 8, 2),  
)

# SQL transformation for full load process
full_load_transformation_sql = """Select emp_no, birth_date,first_name,last_name,
case when gender = 'M' then 'Male' when gender = 'F' then 'Female' end as gender,hire_date  
FROM employees"""                             
                          
# SQL transformation for incremental load process
incremental_transformation_sql = """Select emp_no, birth_date,first_name,last_name,
case when gender = 'M' then 'Male' when gender = 'F' then 'Female' end as gender,hire_date  
    -- FROM employees
    -- WHERE DATE(hire_date) = '{{ ds }}'"""

# Full load process
with full_load_dag:
    # Extract data from MySQL Employees table
    extract_mysql_data = MySqlOperator(
        task_id='extract_data',
        sql='SELECT * FROM employees',
        mysql_conn_id='mysql_conn_id',  
        database='Employees',  
    )

    # Transform data using SQL for full load process
    transform_data_full_load = MySqlOperator(
        task_id='transform_data_full_load',
        sql=full_load_transformation_sql,
        mysql_conn_id='mysql_conn_id',  
        database='Employees',  
    )

    # Load transformed data into BigQuery
    load_bigquery_data = BigQueryOperator(
        task_id='load_bigquery_data',
        sql=transform_data_full_load.sql,  
        destination_dataset_table='dejid.dataset.employees',  
        write_disposition='WRITE_TRUNCATE',  
        bigquery_conn_id='bigquery_conn_id', 
        use_legacy_sql=False,
    )

    extract_mysql_data >> transform_data_full_load >> load_bigquery_data

# Incremental load process
with incremental_dag:
    # Extract data from MySQL Employees table for the past day
    extract_mysql_data = MySqlOperator(
        task_id='extract_data',
        sql="SELECT * FROM employees WHERE DATE(hire_date) = '{{ ds }}'",
        mysql_conn_id='mysql_conn_id', 
        database='Employees',  
    )

    # Transform data using SQL for incremental load process
    transform_data_incremental = MySqlOperator(
        task_id='transform_data_incremental',
        sql=incremental_transformation_sql,
        mysql_conn_id='mysql_conn_id',  
        database='Employees',  
    )

    # Load transformed data into BigQuery (using WRITE_APPEND for incremental load)
    load_bigquery_data = BigQueryOperator(
        task_id='load_bigquery_data',
        sql=transform_data_incremental.sql,  
        destination_dataset_table='dejid.dataset.employees',  
        write_disposition='WRITE_APPEND',  
        bigquery_conn_id='bigquery_conn_id',  
        use_legacy_sql=False,
    )

    extract_mysql_data >> transform_data_incremental >> load_bigquery_data


