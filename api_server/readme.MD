# 개요
- fastapi를 이용한 API 서비스 간단하게 구성
- 해당 서비스는 airflow의 서비슫단에 추가(도커 컴포즈)

# 구동
    uvicorn main:app --host 0.0.0.0 --port 8000

# 접속
    - 홈페이지
        - http://127.0.0.1:8000/
    - 문서 접속(swagger docs)
        - http://127.0.0.1:8000/docs
        - http://127.0.0.1:8000/redoc