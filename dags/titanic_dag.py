from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="titanic_dag",
    start_date=datetime(2026, 4, 15),
    schedule= "0 2 1 * *",
    catchup=True,
    end_date=datetime(2026, 12, 31)

) as dag:

    extraction = BashOperator(
        task_id="extraction",
        bash_command="""
        docker exec spark-jupyter spark-submit /home/jovyan/work/Scripts/extraction.py
        """
    )

    transformation = BashOperator(
    task_id="transformation",
    bash_command="""
    docker exec spark-jupyter spark-submit /home/jovyan/work/Scripts/transformation.py
    """
    )

    loading = BashOperator(
        task_id="loading",
        bash_command="""
        docker exec spark-jupyter spark-submit \
        --packages net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.22 \
        /home/jovyan/work/Scripts/loading.py
        """
    )

    extraction >> transformation >> loading
