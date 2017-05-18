#!/bin/bash

# python code to get caseids from list
CASEIDCOUNT=1
caseids=`python - $CASEIDCOUNT <<END
if __name__ == '__main__':
    import sys
    count = sys.argv[1]
    with open('/home/fbaig/caseids.list') as f:
        caseids = f.readlines()
    caseids = [s.split(',')[0] for s in caseids]
    #caseids = [s.rstrip() for s in caseids]
        
    caseids = caseids[:int(count)]
    #print caseids
    print ('%s') % ','.join(caseids)
END`

#ALGOS=yi-algorithm-v1,yi-algorithm-v11
ALGOS=algo2,algo1
#CASEIDS=$caseids
# small
CASEIDS=TCGA-06-1802-01Z-00-DX1
# bigger
#CASEIDS=TCGA-02-0003-01Z-00-DX2
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



# # hardcoded values for debugging purposes
# algos=' --algos "yi-algorithm-v1,yi-algorithm-v11"'
# # caseIDs=' --caseids '$caseids
# # algos=' --algos "algo1,algo2"'
# # small
# caseIDs=' --caseids "TCGA-06-1802-01Z-00-DX1"'
# # bigger
# # caseIDs=' --caseids "TCGA-02-0003-01Z-00-DX2"'
# heatmapType=' --metric jaccard'
# # input=' --input hdfs'
# # output=' --output client'
# # # mongodb specific params
# # inputdb='--inputdb u24_segmentation'
# # outputdb='' #'--outputdb temp_db'
# # inputcollection='--inputcollection results'
# # outputcollection='' #'--outputcollection temp_col'

# # resultExeID='--result_exe_id fbaig'

# # run spark job
# echo "Executing command $SPARK_HOME/bin/spark-submit --class $className $jar $algos $caseIDs $heatmapType"

# $SPARK_HOME/bin/spark-submit --class $className --conf "spark.driver.extraLibraryPath=/home/fbaig/sbu-bmi-u24/spark-gis/lib" --conf "spark.executor.extraLibraryPath=/home/fbaig/sbu-bmi-u24/spark-gis/lib" --conf "spark.default.parallelism=96" $jar $algos $caseIDs $heatmapType
