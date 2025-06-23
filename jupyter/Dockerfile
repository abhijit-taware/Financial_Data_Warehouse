FROM jupyter/pyspark-notebook:spark-3.5.0

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip && pip install --no-cache-dir -r /requirements.txt
