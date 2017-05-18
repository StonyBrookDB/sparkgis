#!/bin/bash
#
# Generate HeatMap for BMI Data
# The data needs to be orgnaised as follows on HDFS
#    * Specify the data & results directories in ../conf/sparkgis.properties
#        * hdfs-algo-data=/heatmps/data/
#        * hdfs-hm-results=/heatmaps/reults/
#    * Each directory in the 'hdfs-algo-data' corresponds to an algorithm
#        * /heatmaps/data/algorithm-v1/
#        * /heatmaps/data/algorithm-v2/
#        * /heatmaps/data/algorithm-v3/ and so on
#    * Each file in algorithms directory corresponds to a caseID file having spatial information (e.g. id TAB POLYGON)
#        * /heatmaps/data/algorithm-v1/case-id1
#        * /heatmaps/data/algorithm-v1/case-id2
#        * /heatmaps/data/algorithm-v1/case-id3 and so on
#    * To generate heatmap for caseids 'c1234' and 'c6789' with algorithms 'algorithm-v1' and 'algorithm-v2' the following HDFS datapaths must be valid
#        * /heatmaps/data/algorithm-v1/c1234
#        * /heatmaps/data/algorithm-v1/c6789
#        * /heatmaps/data/algorithm-v2/c1234
#        * /heatmaps/data/algorithm-v2/c6789

ALGOS=yi-algorithm-v1,yi-algorithm-v11
CASEIDS=TCGA-06-1802-01Z-00-DX1
METRIC=jaccard
TILESIZE=64

# run spark job
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
LIB_DIR="$SCRIPT_DIR/../lib"
CLASSNAME=driver.SparkGISBMIHeatMap
JAR=$SCRIPT_DIR/../target/uber-sparkgis-1.0.jar

$SPARK_HOME/bin/spark-submit \
    --class $CLASSNAME \
    --conf "spark.driver.extraLibraryPath=$LIB_DIR" \
    --conf "spark.executor.extraLibraryPath=$LIB_DIR" \
    $JAR --algos $ALGOS --caseids $CASEIDS --metric $METRIC --tilesize $TILESIZE
