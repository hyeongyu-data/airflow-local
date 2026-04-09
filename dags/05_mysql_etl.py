'''
- etl 간단하게 적용, 스마트팩토리상 온도 센서에 대한 ETL 처리, mysql 사용
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
# 추가분
from airflow.providers.mysql.operators.mysql import MysqlOperator
# Load 처리시 sql에 전처리된 데이터를 밀어 넣을때 사용
from airflow.providers.mysql.hooks.mysql import MySqlHook
# 데이터
import json
import random
import pandas as pd  # 소량의 데이터(데이터 규모)
import os

# 2. 기본설정
# 프로젝트 내부 폴더를 데이터용으로 (~/dags/data)지정
# task 진행간 생성되는 파일을 동기화하도록 위치 지정 -> 향후 s3(데이터 레이크)로 대체 될수 있음

# 도커 내부에 생성된 컨테이너 상 워커내의 airflow 상의 지정한 데이터 위치
DATA_PATH = '/opt/airflow/dags/data'
os.mkdir(DATA_PATH, exist_ok=True)

def _extract(**kwargs):
    pass
def _trasform(**kwargs):
    pass
def _load(**kwargs):
    pass

# 3. DAG 정의
with DAG(
    dag_id      = "05_mysql_etl", 
    description = "etl 수행하여 mysql에 온도 센서 데이터 적제",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '@daily',
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['mysql', 'etl'],
) as dag:
    # 4. task 정의
    task_create_table = MysqlOperator(
        # 최초는 생성, 존재하면 pass => if not exists
        task_id = "create_table",
    )
    task_extract    = PythonOperator(
        task_id = "extract",
        python_callable = _extract
    )
    task_trasform   = PythonOperator(
        task_id = "trasform",
        python_callable = _trasform
    )
    task_load       = PythonOperator(
        task_id = "load",
        python_callable = _load
    )

    # 5. 의존성 정의 -> 시나리오별 준비 
    task_create_table >> task_extract >> task_trasform >> task_load