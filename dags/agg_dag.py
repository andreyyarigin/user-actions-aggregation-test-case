from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
    'weekly_spak_agg',
    default_args=default_args,
    description='run spark_agg_task daily at 7:00',
    schedule_interval='0 7 * * *', 
    start_date=datetime.now(),
    catchup=False,
) as dag:

    run_spark_agg_task = DockerOperator(
        task_id='spark_agg_task',
        image='spark_agg',  
        api_version='auto',
        auto_remove=True,
        command='spark-submit /workdir/spark_agg_script.py "{{ ds }}"',
        docker_url='tcp://docker-socket-proxy:2375',
        network_mode='bridge', 
        mount_tmp_dir=False,
        mounts=[
            Mount( 
                source="/Users/andreyyarigin/projects/user-actions-aggregation-test-case/input", 
                target="/opt/spark/input/",
                type="bind",
            ),
            Mount(
                source="/Users/andreyyarigin/projects/user-actions-aggregation-test-case/output",
                target="/opt/spark/output/",
                type="bind",
            ),
            Mount(
                source="/Users/andreyyarigin/projects/user-actions-aggregation-test-case/spark_output_tmp",
                target="/opt/spark/spark_output_tmp/",
                type="bind",
            )
        ],
    )

    run_spark_agg_task
