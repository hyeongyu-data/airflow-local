# 재플린에서 flink 인터프린터 상황에서 파이썬 사용시 선언문
%flink.pyflink

'''
# 내부적으로 준비되어 있음 -> 바로 사용 가능
setting = EnvironmentSettings.new_instance().in_streaming_mide().build()
t_env = TableEnvironment.create( setting )
'''
# st_env -> 자체적으로 준비 되어있는 객체 사용(전역변수)
# 1. 입력 테이블 정의
st_env.execute_sql("""
    create table stock_input (
        ticker STRING,
        price DOUBLE,
        event_time TIMESTAMP(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
    ) with (
        "connector"           = "kinesis",
        "stream"              = "de-ai-03-an2-kds-stock-input",
        "aws.region"          = "ap-northeasr-2",
        "scan.stream.initpos" = "LATEST",
        "format"              = "json"
    )
""")

# 2. 출력 테이블 정의
st_env.execute_sql("""
    create table stock_output (
        ticker STRING,
        avg_price DOUBLE,
        window_time TIMESTAMP(3)
    ) with (
        "connector"           = "kinesis",
        "stream"              = "de-ai-03-an2-kds-stock-output",
        "aws.region"          = "ap-northeasr-2",
        "format"              = "json"
    )
""")

# 3. 연산처리(10초 단위/티커로 데이터를 그룹화, 평균집계)
st_env.execute_sql("""
    INSERT INTO stock_output
    SELECT
        ticker,
        AVG(price) as avg_price,
        TUMBLE_END(event_time, INTERVAL '10' SECOND) as window_time
    FROM
        stock_input
    GROUP BY TUMBLE(event_time, INTERVAL '10' SECOND), ticker
""")