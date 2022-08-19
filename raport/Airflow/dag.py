from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="Report_test", 
        description="This DAG runs a simple Pyspark app.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

task_2 = SparkSubmitOperator(
    task_id="task_2",
    application="/usr/local/spark/app/task_2.py", # Spark application path created in airflow and spark cluster
    name="Task 2",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    dag=dag)

task_4 = SparkSubmitOperator(
    task_id="task_4",
    application="/usr/local/spark/app/task_4.py", # Spark application path created in airflow and spark cluster
    name="Task 4",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    dag=dag)

task_3 = SparkSubmitOperator(
    task_id="task_3",
    application="/usr/local/spark/app/task_3.py", # Spark application path created in airflow and spark cluster
    name="Task 3",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    dag=dag)

task_5 = SparkSubmitOperator(
    task_id="task_5",
    application="/usr/local/spark/app/task_5.py", # Spark application path created in airflow and spark cluster
    name="Task 5",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    dag=dag)

task_6 = SparkSubmitOperator(
    task_id="task_6",
    application="/usr/local/spark/app/task_6.py", # Spark application path created in airflow and spark cluster
    name="Task 6",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    dag=dag)

report = SparkSubmitOperator(
    task_id="report",
    application="/usr/local/spark/app/report.py", # Spark application path created in airflow and spark cluster
    name="Report",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> [task_2, task_3, task_4, task_5, task_6] >> report >> end 