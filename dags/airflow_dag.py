# An airflow DAG

# For DAG file to be visible from scheduler/webserver you need to add it to dags_folders
# specified in airflow.cfg file.Therefore in airflow.cfg modify the following
# dags_folder = ~/FPL/dags

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


#define arguments
args={
    'owner':'pkaram',
    'depends_on_past':False,
    'email_on_failure':False,
    'retries':0,
    'start_date':days_ago(0)
}

#define DAG
dag=DAG(
    dag_id='fpl_db',
    default_args=args,
    description='FPL data ETL',
    schedule_interval=timedelta(days=1)
)

#task for etl
create_db=BashOperator(
    task_id='db_update',
    bash_command='python {add your path}/create_update_db.py',
    dag=dag
)

#create clusters based on information for the players
player_clustering=BashOperator(
    task_id='cluster_players',
    bash_command='python {add your path}/cluster_players.py',
    dag=dag
)

#task pipeline
create_db >> player_clustering