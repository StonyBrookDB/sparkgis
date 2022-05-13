FROM apache/spark-py:v3.2.1
#FROM bitnami/spark:3-debian-10

USER root
RUN apt-get update && apt-get install python3-matplotlib -y
WORKDIR /usr/src/app
COPY requirements.txt ./

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

WORKDIR /code
###########################################
#FROM ubuntu

# RUN apt-get update && apt-get install openjdk-8-jdk -y && apt-get install maven -y
# RUN apt-get update && apt-get install python3.8 -y && apt-get install python3-pip -y
# RUN apt-get install python3-matplotlib
# #RUN ln -s /usr/bin/python3.8 /usr/bin/python3

# WORKDIR /usr/src/app
# COPY requirements.txt ./

# RUN pip install --no-cache-dir --upgrade pip && \
#     pip install --no-cache-dir -r requirements.txt