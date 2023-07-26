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

dag = DAG('C360_HA_DHSXKD_giangnd', description='C360_HA_DHSXKD_giangnd',
          schedule_interval="18 09 * * *", start_date=datetime(2022, 6, 20),
          catchup=False)

day = "{{ (data_interval_end.astimezone(dag.timezone) - macros.timedelta(days=1)).strftime('%Y%m%d')}}"
day2 = "{{ (data_interval_end.astimezone(dag.timezone) - macros.timedelta(days=2)).strftime('%Y%m%d')}}"
day3 = "{{ (data_interval_end.astimezone(dag.timezone) - macros.timedelta(days=3)).strftime('%Y%m%d')}}"

local_path_129 = '/u01/deploy/customer360/03.ingest/giangnd'



run_ingest_cttno = BashOperator(
    task_id='ingest_ctno2clickhouse',
    bash_command='bash '+local_path_129 +'/CtNo.sh '+day2 +' ' ,
    queue='bigdata129',
    retries=0,
    dag=dag
)

process_staging = BashOperator(
    task_id='process_rawstaging_tbl',
    bash_command='bash '+local_path_129 +'/staging_processraw.sh '+day2 +' ' ,
    queue='bigdata129',
    retries=0,
    dag=dag
)

staging_madoicap = BashOperator(
    task_id='staging_madoicap',
    bash_command='bash '+local_path_129 +'/staging_madoicap.sh '+day2 +' ' ,
    queue='bigdata129',
    retries=0,
    dag=dag
)

staging_nhanvien = BashOperator(
    task_id='staging_nhanvien',
    bash_command='bash '+local_path_129 +'/staging_nhanvien.sh '+day2 +' ' ,
    queue='bigdata129',
    retries=0,
    dag=dag
)

staging_thuebaotratruoc = BashOperator(
    task_id='staging_thuebaotratruoc',
    bash_command='bash '+local_path_129 +'/staging_thuebaotratruoc.sh '+day2 +' ' ,
    queue='bigdata129',
    retries=0,
    dag=dag
)

aggregate_subscriber_dhsxkd = BashOperator(
    task_id='aggregate_subscriber_dhsxkd',
    bash_command='bash '+local_path_129 +'/subscriber_dhsxkd.sh '+day2 +' ' ,
    queue='bigdata129',
    retries=0,
    dag=dag
)

process_staging >> [staging_madoicap, staging_nhanvien, staging_thuebaotratruoc] >> aggregate_subscriber_dhsxkd