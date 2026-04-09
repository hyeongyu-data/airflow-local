'''
- etl 간단하게 적용, 스마트팩토리상 온도 센서에 대한 ETL 처리, mysql 사용
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# 2. DAG 정의
with DAG() as dag:
    # 3. task 정의
    task_extract    = PythonOperator()
    task_trasform   = PythonOperator()
    task_load       = PythonOperator()

    # 4. 의존성 정의 -> 시나리오별 준비 
    task_extract >> task_trasform >> task_load