#!/bin/bash

LOCAL_DATA_PATH=$PWD/../data/
LOCAL_CODE_PATH=$PWD/../code/

NAME=sparkgis

docker build -t $NAME .

docker run -it \
       --name $NAME \
       -v $LOCAL_DATA_PATH:/data/ \
       -v $LOCAL_CODE_PATH:/code/ \
       $NAME bash

#SPARK_PATH=/shared/spark/spark-3.1.1-bin-hadoop2.7/

# docker run -it \
# --name $NAME \
# -v $LOCAL_DATA_PATH:/shared/ \
# -e SPARK_HOME=$SPARK_PATH \
# $NAME bash
