import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

weekday_person_to_email = {
    0: "Bob",
    1: "Joe",
    2: "Alice",
    3: "Joe",
    4: "Alice",
    5: "Alice",
    6: "Alice"
}

args = {"owner": "bkersbergen",
        "start_date": airflow.utils.dates.days_ago(14),
        "scheduling_interval": "0 0 * * *"
        }

dag = DAG(
    dag_id="ex3",
    default_args=args
)


def __print_weekday(execution_date, **context):
    print(execution_date.strftime('%d'))


print_weekday = PythonOperator(
    task_id="print_weekdate",
    python_callable=__print_weekday,
    provide_context=True,
    dag=dag
)


def __determine_who_to_email(execution_date, **context):
    name = weekday_person_to_email[execution_date.strftime('%d')]
    return "email_{}".format(name)


branching = BranchPythonOperator(
    task_id="determine_how_to_email",
    python_callable=__determine_who_to_email,
    provide_context=True,
    dag=dag
)

print_weekday >> branching

final_task = DummyOperator(
    task_id="final_task",
    dag=dag,
    trigger_rule=TriggerRule.ONE_SUCCESS
)

for name in set(weekday_person_to_email.values()):
    task_id = "email_{}".format(name)
    email_taks = DummyOperator(task_id=task_id, dag=dag)
    branching >> email_taks >> final_task
