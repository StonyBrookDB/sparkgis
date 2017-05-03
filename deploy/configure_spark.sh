#!/bin/bash

# (1) $SPARK_HOME/conf/slaves
#     - add hostnames for slave nodes for spark (line by line)
#     - (if not) change $SPARK_HOME/conf/slaves.template to $SPARK_HOME/conf/slaves 
# (2) $SPARK_HOME/conf/spark-defaul-conf
#     - modify spark.master
#     - add spark.default.parallelism
#     - add spark.driver.extraLibraryPath - point to 'lib' directory in SparkGIS source
#     - add spark.executor.extraLibraryPath - point to 'lib' directory in SparkGIS source
# (3) Start spark by executing $SPARK_HOME/sbin/start-all
    
