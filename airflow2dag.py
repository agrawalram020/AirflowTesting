from datetime import datetime, timedelta
from airflow import DAG
# BREAKING 1: SubDagOperator is removed in Airflow 3
from airflow.operators.subdag import SubDagOperator 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# BREAKING 2: days_ago is removed in Airflow 3
from airflow.utils.dates import days_ago 
# BREAKING 3: Direct DB access tools are restricted/removed for workers
from airflow.utils.session import provide_session
from airflow.models import TaskInstance
import pendulum

def subdag_factory(parent_dag_name, child_dag_name, args):
    """
    SubDAGs are removed in Airflow 3. Use TaskGroups instead.
    """
    with DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=args,
        schedule_interval="@daily",
    ) as dag:
        BashOperator(
            task_id="subdag_task",
            bash_command="echo 'I will fail in Airflow 3'"
        )
    return dag

# BREAKING 4: Direct DB Access is strictly blocked from Workers in Airflow 3
@provide_session
def unsafe_db_access(session=None, **context):
    # This query will fail because workers no longer have direct access 
    # to the metadata database connection string.
    tis = session.query(TaskInstance).limit(5).all()
    print(f"Found {len(tis)} tasks")

def print_deprecated_context(**context):
    # BREAKING 5: 'execution_date' is removed from the context dictionary.
    # It is replaced by 'logical_date'.
    print(f"Execution Date: {context['execution_date']}")
    
    # BREAKING 6: 'next_ds', 'prev_ds', 'tomorrow_ds', 'yesterday_ds' 
    # are removed. You must use standard Jinja filters or pendulum math.
    print(f"Next execution: {context['next_ds']}")

default_args = {
    'owner': 'airflow',
    'email': ['test@example.com'],
    'start_date': pendulum.datetime(2026, 1, 1)
    # BREAKING 7: 'email' parameter deprecated in core, moved to providers
}

with DAG(
    dag_id='airflow_2_to_3_migration_test',
    default_args=default_args,
    # BREAKING 8: days_ago() is removed. Use pendulum.today('UTC').add(days=-2)
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), 
    # BREAKING 9: 'schedule_interval' is deprecated. Use 'schedule' instead.
    schedule_interval='@daily',
    # BREAKING 10: catchup defaults to False in Airflow 3 config. 
    # Explicitly setting it is safer, but relying on default True will change behavior.
    catchup=True, 
    tags=['migration_test'],
) as dag:

    # 1. Test DB Access Removal
    db_task = PythonOperator(
        task_id='test_db_access',
        python_callable=unsafe_db_access
    )

    # 2. Test Context Variable Removal
    context_task = PythonOperator(
        task_id='test_context_variables',
        python_callable=print_deprecated_context
    )

    # 3. Test SubDAG Removal
    subdag_task = SubDagOperator(
        task_id='test_subdag',
        subdag=subdag_factory(
            'airflow_2_to_3_migration_test',
            'test_subdag',
            default_args
        )
    )

    # 4. Test Deprecated Template Variables
    # 'execution_date' in templates is removed. Use {{ logical_date }} or {{ ds }}
    template_task = BashOperator(
        task_id='test_jinja_templates',
        bash_command="echo 'Run date: {{ execution_date }}'"
    )

    db_task >> context_task >> subdag_task >> template_task
