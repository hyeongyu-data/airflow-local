'''
- Amazon Data Firehose(ADF)에게 direct로 데이터를 put 샘플
'''

# apache-airflow-providers-amazon 설치하여 자동으로 aws sdk(boto3)가 자동설치되어 있음
# 1. 모듈 가져오기
import boto3
import json
import time

# 2. 환경변수