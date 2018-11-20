import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

args = {"owner": "bkersbergen",
        "start_date": airflow.utils.dates.days_ago(3)
        }
# opgegeven via de airflow admin webui admin interface
# host:178.62.227.89
# username: gdd
# db: gdd
# pass: supergeheim123abc!

dag = DAG(
    dag_id="ex4",
    default_args=args,
    schedule_interval="0 0 * * *"
)


psql_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="read_postgres",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="mr_ds",
    filename="price_paid_uk/{{ ds }}/registry.json",
    postgres_conn_id="my_postgres_conn_id",
    dag=dag
)


