from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import date
from datetime import datetime, timedelta
from airflow.models.baseoperator import chain


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import date
from datetime import datetime, timedelta

dag = DAG('khalt_uc6', description='khalt_uc6',
          schedule_interval="0 7 * * *", start_date=datetime(2022, 6, 20),
          catchup=False)

local_path_135 = '/u30/deploy/khalt/job/UC6'


start_trigger = BashOperator(
    task_id='start_trigger',
    bash_command='bash '+local_path_135 +'/jobdaily.sh ',
    queue='bigdata135',
    retries=0,
    dag=dag
)

start_trigger