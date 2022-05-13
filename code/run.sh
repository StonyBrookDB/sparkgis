#!/bin/bash

# local spark setup
export PYSPARK_PYTHON=python3
$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    sparkgis_driver.py
