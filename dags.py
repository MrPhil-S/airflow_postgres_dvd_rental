import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from transformation import *

#define the etl function
def etl():
        payment_df = extract_payments_to_df()
        staff_df = extract_staff_to_df()
        transformed_df = transform_avg_amount(payment_df,staff_df)
        load_df_to_db(transformed_df)

#define the arguments for the load_df_to_db
default_args = {
'owner': 'airflow',
'start_date': airflow.utils.dates.days_ago(1),
'depends_on_past': True,
'email': ['nopschims@gmail.com'],
'email_on_failure': True,
'email_on_retry': False,
'retries': 3,
'retry_delay': timedelta(seconds=10),
}

#intantiate the DAG using the DAG class
dag = DAG(dag_id = "etl_pipeline",
          default_args = default_args,
          schedule_interval = "1 0 * * *")
                #min, hour, dayofmonth, week, dayofweek

#define the ETL task
etl_task = PythonOperator(task_id="etl_task",
                          python_callable = etl,
                          dag=dag)

etl()
