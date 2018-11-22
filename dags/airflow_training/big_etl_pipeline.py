import airflow
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

args = {"owner": "bkersbergen",
        "start_date": airflow.utils.dates.days_ago(3)
        }
# user en pass opgegeven via de airflow admin webui admin interface

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

cluster_name = "mr_ds-{{ ds }}"
gcs_project_id = "airflowbolcom-29ec97aeb308c0dd"

create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc_cluster",
    cluster_name=cluster_name,
    project_id=gcs_project_id,
    num_workers=2,
    zone="europe-west4-a",
    dag=dag
)


psql_to_gcs >> create_cluster