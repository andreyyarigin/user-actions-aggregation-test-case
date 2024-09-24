FROM bitnami/spark:latest

COPY requirements.txt spark_agg_script.py /workdir/
WORKDIR /workdir

RUN pip install -r /workdir/requirements.txt 