#!/bin/bash
USAGE="sh run_spark_gis.sh '--uid [32-bit id]' '--algos '[csv-algos-list]'' '--caseids '[csv-caseID-list]'' '--metric [jaccard|dice|tile_dice]' '--input [hdfs|mongodb]' '--output [hdfs|mongodb|client]' '--result_exe_id exe-id'"

if [ "$#" -lt 6 ]
then
    echo $USAGE
    exit
fi

uid=`echo $1 | tr -d "'"`
algos=`echo $2 | tr -d "'"`
caseIDs=`echo $3 | tr -d "'"`
heatmapType=`echo $4 | tr -d "'"`
input=`echo $5 | tr -d "'"`
output=`echo $6 | tr -d "'"`
resultExeID=`echo $7 | tr -d "'"`

# hardcoded values for debugging purposes
className=sparkgis.SparkGISMain
jar=/home/fbaig/spark-gis/spark-gis-final-backup/spark-gis/cheuk/spark-gis-prod/spark-gis/target/uber-sparkgis-1.0.jar
# command line arguments
# uid='--uid 123456'
# algos=' --algos "yi-algorithm-v1,yi-algorithm-v11"'
# caseIDs=' --caseids "TCGA-02-0001-01Z-00-DX1"'
# heatmapType=' --metric jaccard'
# input=' --input hdfs'
# output=' --output client'

#echo $USAGE

# run spark job


echo "Executing command $SPARK_PATH/bin/spark-submit --class $className $jar $algos $caseIDs $heatmapType $input $output $resultExeID"

$SPARK_PATH/bin/spark-submit --class $className $jar $algos $caseIDs $heatmapType $input $output $resultExeID
