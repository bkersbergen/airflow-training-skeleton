import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {"owner": "bkersbergen", "start_date": airflow.utils.dates.days_ago(14)}
dag = DAG(
    dag_id="ex3",
    default_args=args
)

weekday_person_to_email = {
    0: "bob",
    1: "joe",
    2: "alice",
    3: "joe",
    4: "alice",
    5: "alice",
    6: "alice",
}


def __determine_who_to_email(execution_date, **context):
    weekday_id = execution_date.strftime('%a')
    name = weekday_person_to_email[weekday_id]
    if not name:
        from airflow import AirflowException
        raise AirflowException('{} not in weekday_person_to_email'.format(weekday_id))
    return name


branching = BranchPythonOperator(
    task_id='branching', python_callable=__determine_who_to_email, provide_context=True, dag=dag
)

for person in set(weekday_person_to_email.values()):
    branching >> DummyOperator(task_id=person, dag=dag)

t1 = BashOperator(
    task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
)
