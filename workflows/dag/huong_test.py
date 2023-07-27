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

dag = DAG('huongtest1', description='huongtest1',
          schedule_interval="0 10 * * *", start_date=datetime(2022, 6, 20),
          catchup=False)

day = "{{ (data_interval_end.astimezone(dag.timezone) - macros.timedelta(days=1)).strftime('%Y%m%d')}}"
day2 = "{{ (data_interval_end.astimezone(dag.timezone) - macros.timedelta(days=2)).strftime('%Y%m%d')}}"


local_path_121 = '/u01/deploy/npom/03.ingest/live/analytic_job/job_script/'

run_integration_sub_imei = BashOperator(
    task_id='integration_sub_imei',
    bash_command='echo '+local_path_121 +'/integration/start_integration_sub_imei.sh '+day +' ' ,
    queue='bigdata121',
    dag=dag
)

run_integration_sub_gsma = BashOperator(
    task_id='integration_sub_gsma',
    bash_command='echo '+local_path_121 +'/integration/start_integration_sub_gsma.sh '+day +' ' ,
    queue='bigdata121',
    dag=dag
)

run_integration_rims = BashOperator(
    task_id='integration_rims',
    bash_command='echo '+local_path_121 +'/integration/dailyJobRims.sh '+day +' ' ,
    queue='bigdata121',
    dag=dag
)

run_integration_ggsn = BashOperator(
    task_id='integration_ggsn',
    bash_command='echo '+local_path_121 +'/integration/dailyJobGgsn.sh '+day +' ' ,
    queue='bigdata121',
    dag=dag
)

run_integration_voice_sms_filter = BashOperator(
    task_id='integration_voice_sms_filter',
    bash_command='echo '+local_path_121 +'/integration/dailyJobVoiceSmsFilter.sh '+day +' ' ,
    queue='bigdata121',
    dag=dag
)

run_aggregation_roaming_package = BashOperator(
    task_id='statistic_roaming_package_daily',
    bash_command='echo '+local_path_121 + 'aggregation/run_job19_statistic_roaming_package.sh '+ day2 +' '+ day2 + ' daily ' + ' ',
    queue='bigdata121',
    dag=dag
)

run_aggregation_roaming_ggsn = BashOperator(
    task_id='statistic_roaming_ggsn_daily',
    bash_command='echo '+local_path_121 + 'aggregation/run_job21_statistic_ggsn_roaming_data.sh '+ day +' '+ day + ' daily ' + ' ',
    queue='bigdata121',
    dag=dag
)

run_aggregation_roaming_voice = BashOperator(
    task_id='statistic_roaming_voice_daily',
    bash_command='echo '+local_path_121 + 'aggregation/run_job21_statistic_voice_roaming_data.sh '+ day +' '+ day + ' daily '+ ' ' ,
    queue='bigdata121',
    dag=dag
)

run_aggregation_roaming_sms = BashOperator(
    task_id='statistic_roaming_sms_daily',
    bash_command='echo '+local_path_121 + 'aggregation/run_job21_statistic_sms_roaming_data.sh '+ day +' '+ day + ' daily' + ' ',
    queue='bigdata121',
    dag=dag
)

run_merge_roaming_all = BashOperator(
    task_id='merge_roaming_all_daily',
    bash_command='echo '+local_path_121 + 'aggregation/run_job21_daily_merge_all.sh '+ day +' '+ day + ' ' ,
    queue='bigdata121',
    dag=dag
)

run_merge_roaming_province = BashOperator(
    task_id='merge_roaming_province_daily',
    bash_command='echo '+local_path_121 + 'aggregation/run_job21_daily_merge_province_roaming.sh '+ day +' '+ day +' ' ,
    queue='bigdata121',
    dag=dag
)

#DAG



#DAGrun_integration_ggsn
#DAGrun_integration_voice_sms_filter

# Dags
run_integration_sub_imei>>run_integration_sub_gsma 
run_integration_rims >> [run_integration_ggsn, run_integration_voice_sms_filter] 
run_integration_ggsn >> run_aggregation_roaming_ggsn
run_integration_voice_sms_filter >> [run_aggregation_roaming_voice, run_aggregation_roaming_sms] 
run_aggregation_roaming_ggsn >> [run_merge_roaming_all, run_merge_roaming_province]
run_aggregation_roaming_voice >> [run_merge_roaming_all, run_merge_roaming_province]
run_aggregation_roaming_sms >> [run_merge_roaming_all, run_merge_roaming_province]