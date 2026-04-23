'''
DAG에서 opensearch 검색 -> 데이터 획득
'''

# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch
import pendulum # 서울 시간대 간편하게 설정
from airflow.models import Variable

# 2. 환경변수
# HOST, AUTH, 인덱스(상황에 따라 별도 구성가능함) -> 검색어/패턴으로 구성/고정
HOST = Variable.get("OPENSEARCH_HOST")
AUTH = (Variable.get("AUTH_NAME"),Variable.get("AUTH_PW"))
print(HOST,AUTH)

# 4-1. opensearch를 통해 검색 후 결과 획득 콜백함수( _searching_proc )
def _searching_proc(**kwargs):
    pass

# 3. DAG 정의
with DAG(
    dag_id      = "13_opensearch_ready", 
    description = "검색엔진에 대해 질의 후 결과 획득",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '*/10 * * * *',
    start_date  = pendulum.datetime(26,1,1,tz="Asis/Seoul"), # 서울 시간대 1월 1일
    catchup     = False,
    tags        = ['aws', 'opensearch'],
) as dag:
    # 4. task 정의
    task = PythonOperator(
        task_id = 'searching_proc',
        python_callable = _searching_proc
    )

    # 5. 의존성
    # task