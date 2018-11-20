import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {"owner": "bkersbergen", "start_date": airflow.utils.dates.days_ago(14)}
dag = DAG(
    dag_id="myfirstjob",
    default_args=args
)


def __my_python_function(execution_date, **context):
    print(execution_date)
    print(context['ds'])
    print(context['params']['mykey'])


pp = PythonOperator(
    task_id='my function',
    python_callable=__my_python_function,
    params={"mykey": 5},
    provide_context=True,
    dag=dag
)

t1 = BashOperator(
    task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
)

t2 = BashOperator(
    task_id="wait_5", bash_command="sleep 5", dag=dag
)

t3 = BashOperator(
    task_id="wait_10", bash_command="sleep 10", dag=dag
)

t4 = BashOperator(
    task_id="wait_1", bash_command="sleep 1", dag=dag
)

t5 = BashOperator(
    task_id="print_done", bash_command="echo done", dag=dag
)

t1 >> [t2, t3, t4] >> t5

