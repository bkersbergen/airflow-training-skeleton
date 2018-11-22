import airflow
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, \
    DataprocClusterDeleteOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

'''
Toy airflow pipeline that I wrote at the airflow training
Common tasks are in this pipeline like reading postgres, storing in google storage, create and delete dataproc cluster etc.


'''

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

cluster_name = "mrds{{ ds_nodash }}"
gcs_project_id = "airflowbolcom-29ec97aeb308c0dd"

create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc_cluster",
    cluster_name=cluster_name,
    project_id=gcs_project_id,
    zone="europe-west4-a",
    num_workers=2,
    dag=dag
)

build_statistics = DataProcPySparkOperator(
    task_id="build_statistics",
    main="gs://mr_ds/pyspark/build_statistics.py",
    cluster_name=cluster_name,
    project_id=gcs_project_id,
    zone="europe-west4-a",
    arguments=["{{ ds }}"],
    dag=dag
)

delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc_cluster",
    cluster_name=cluster_name,
    project_id=gcs_project_id,
    zone="europe-west4-a",
    trigger_rule=TriggerRule.ALL_DONE,   # always execute the deletion of the cluster even if the 'build_statistics' fail
    dag=dag
)

psql_to_gcs >> create_cluster >> build_statistics >> delete_cluster


