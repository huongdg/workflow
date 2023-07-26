# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# +
from datetime import datetime, timedelta
from textwrap import dedent
import json
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

# +
dag = DAG(
    "vega-spark-jobs",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple running spark job via API",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 7, 18),
    catchup=False,
    tags=["test"],
)



# +
def print_hello():
    return 'Start DAG'


start_task = DummyOperator(task_id='start_task', dag=dag)


# +
http = "https://cpd-cp4dmedia.apps.cp4d.datalake.vnpt.vn/v4/analytics_engines"
endpointPost = '964cbd82-b848-4e80-8bae-90f19517ced1/spark_applications'
headersPost = {"Authorization": "Bearer some_token", "Content-Type": "application/json"}
data_raw = {
    "template_id": "spark-3.3-jaas-v2-cp4d-template",
    "application_details": {
         "application": "/thuvien/vnpt-dailystats-0.25.jar",
        "conf": {
            "spark.executor.memory": "12G",
            "spark.driver.memory": "4G",
            "spark.datasource.singlestore.password": "",
            "ae.spark.executor.count": "20",
            "spark.driver.cores": "2",
            "spark.datasource.singlestore.user": "vnptmedia_ingestion",
            "spark.executor.cores": "2"
        },
        "driver-class-path": "/thuvien/singlestore-spark-connector_2.12-4.1.3-spark-3.3.0.jar,/thuvien/mariadb-java-client-3.1.4.jar,/thuvien/singlestore-jdbc-client-1.1.5.jar,/thuvien/commons-dbcp2-2.9.0.jar",
        "jars": "/thuvien/singlestore-spark-connector_2.12-4.1.3-spark-3.3.0.jar,/thuvien/mariadb-java-client-3.1.4.jar,/thuvien/singlestore-jdbc-client-1.1.5.jar,/thuvien/commons-dbcp2-2.9.0.jar,/thuvien/commons-pool2-2.11.1.jar,/thuvien/spray-json_2.10-1.2.5.jar",
        "class": "vn.vega.vnpt.cdrstats.app.DataRawStats",
        "arguments": [
            "vnptmedia_raw.huongdg_stat_daily",
            "s3_file_date",
            "20230501",
            "20230531"
        ]
    },
    "volumes": [
        {
            "name": "cp4dmedia::huongdg-new-code",
            "mount_path": "/thuvien"
        }
    ]

}
# -

dataPost = json.dumps(data_raw)


spark_submit = SimpleHttpOperator(
    task_id="submit_job",
    http_conn_id="http_cp4d",
    endpoint = endpointPost,    
    method="POST",
    data = dataPost,
    headers=headersPost,
    dag=dag
)


# +
def process_response(**kwargs):
    ti = kwargs['ti']
    response_data = json.loads(ti.xcom_pull(task_ids='submit_job'))["application_id"]
    # Process the response_data as needed
    return response_data
    
# Create a downstream task that processes the HTTP response
process_response_task = PythonOperator(
    task_id='process_response_task',
    python_callable=process_response,
    provide_context=True,
    dag=dag,
)

# -

endpointGET = "964cbd82-b848-4e80-8bae-90f19517ced1/spark_applications/{application_id}"
headersGET = {"Authorization": "Bearer <token>"}

get_status_spark_job = SimpleHttpOperator(
    task_id="get_status_spark_job",
    http_conn_id="http_cp4d",
    endpoint = endpointGET.format(application_id='{{ ti.xcom_pull(task_ids=\'process_response_task\') }}'),    
    method="GET",
    headers=headersGET,
    dag=dag
)

start_task >> spark_submit >> process_response_task >> get_status_spark_job
