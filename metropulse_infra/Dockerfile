# ./metropulse_infra/Dockerfile

FROM spark:4.0.1

USER root

COPY requirements.txt /opt/spark/requirements.txt

RUN pip install --upgrade pip && \
    pip install -r /opt/spark/requirements.txt

RUN pip install psycopg2-binary pandas boto3
