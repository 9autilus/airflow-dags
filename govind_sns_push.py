import datetime
import json
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="govind_sns_push",
    start_date=datetime.datetime(2023, 1, 1),  # Just some sensible date 
    schedule_interval=None,  # This DAG is only triggered manualy
    max_active_runs=2,  # Max dag-run instances running in parallel
    concurrency=4,  # Max task instances running in parallel
)

def _block_2():
    region_name = "us-west-2"
    topic_arn = "arn:aws:sns:us-west-2:628696693764:govind-sns-topic-0"
    sess = boto3.Session(region_name=region_name)
    client = sess.client('sns', region_name=region_name)  # Client
    msg = json.dumps({"my_time": str(datetime.datetime.now())})
    resp = client.publish(TopicArn=topic_arn, Message=msg)
    print(f"Response: {resp}")
    return

blk_sync_done = PythonOperator(
    task_id="block_2",
    python_callable=_block_2,
    dag=dag,
)


