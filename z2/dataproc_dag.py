import uuid
import datetime
from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
)

YC_DP_AZ = 'ru-central1-a'
YC_DP_SSH_PUBLIC_KEY = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIIRvg8+Q3YJQlMRpuVf1DbCSGJXWq+guw7WpiiViNuPk'
YC_DP_FOLDER_ID = 'b1g2fuapen5lho94acc2'     
YC_DP_SUBNET_ID = 'e9bcdfoa2v5rssmq24p9'       
YC_DP_SA_ID = 'ajelhha59q59ap720ihh'          
YC_SOURCE_BUCKET = 'safedisk'                  

# Настройки DAG
with DAG(
        'yandex_dataproc_automation_keep_alive',
        schedule_interval=None,
        tags=['data-processing-and-airflow'],
        start_date=datetime.datetime(2026, 6, 1),
        max_active_runs=1,
        catchup=False
) as ingest_dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        folder_id=YC_DP_FOLDER_ID,
        cluster_name=f'tmp-dp-{str(uuid.uuid4())[:8]}', 
        cluster_description='Временный кластер для выполнения PySpark-задания под оркестрацией Managed Service for Apache Airflow™',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_SOURCE_BUCKET, 
        zone=YC_DP_AZ,
        cluster_image_version='2.1', 
        masternode_resource_preset='s2.small',
        masternode_disk_type='network-ssd',
        masternode_disk_size=20,
        computenode_resource_preset='s2.small',
        computenode_disk_type='network-ssd',
        computenode_disk_size=20,
        computenode_count=1, 
        computenode_max_hosts_count=2,
        services=['YARN', 'SPARK'],
        datanode_count=0,
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='run_pyspark_job',
        main_python_file_uri=f's3a://{YC_SOURCE_BUCKET}/scripts/bitcoin_analytics.py',
        args=[
            f's3a://{YC_SOURCE_BUCKET}/input_data/*.csv',
            f's3a://{YC_SOURCE_BUCKET}/pyspark_result/'
        ],
    )

    create_spark_cluster >> poke_spark_processing
