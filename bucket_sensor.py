from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectUpdatedSensor
import datetime as dt
from airflow.models import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator

lasthour = dt.datetime.now() - dt.timedelta(hours=1)

input_bucket = 'raw_job_data'
input_file = 'gsearch_jobs.csv'

args = {
 'owner': 'airflow',
 'start_date': lasthour,
 'depends_on_past': False,
}
dag = DAG(
 dag_id='GCS_sensor_dag',
 schedule_interval=None,
 default_args=args
)
GCS_File_list = GoogleCloudStorageListOperator(
                    task_id= 'list_Files',
                    bucket= input_bucket,
                    prefix=input_file,
                    delimiter='.csv',
                    dag = dag
                )
file_sensor = GoogleCloudStorageObjectUpdatedSensor(
    task_id='gcs_polling',
    bucket=input_bucket,
    object=input_file,
    dag=dag
)

trigger = TriggerDagRunOperator(
                    task_id='trigger_dag_',
                    trigger_dag_id="on-demand-extract-transform-load-to-bq",
                    dag=dag
                )

file_sensor >> GCS_File_list >> trigger