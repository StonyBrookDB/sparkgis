#!/bin/bash
USAGE="sh run_spark_gis.sh '--uid [32-bit id]' '--algos '[csv-algos-list]'' '--caseids '[csv-caseID-list]'' '--metric [jaccard|dice|tile_dice]' '--input [hdfs|mongodb]' '--output [hdfs|mongodb|client]' '--result_exe_id exe-id'"

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

# hardcoded values for debugging purposes
className=sparkgis.SparkGISMain
jar=target/uber-spark-gis-1.0.jar
# command line arguments
uid='--uid 123456'
algos=' --algos "yi-algorithm-v1,yi-algorithm-v11"'
caseIDs=' --caseids '$caseids
heatmapType=' --metric jaccard'
input=' --input hdfs'
output=' --output client'
# mongodb specific params
inputdb='--inputdb u24_segmentation'
outputdb='' #'--outputdb temp_db'
inputcollection='--inputcollection results'
outputcollection='' #'--outputcollection temp_col'

resultExeID='--result_exe_id fbaig'

# run spark job
echo "Executing command $SPARK_HOME/bin/spark-submit --class $className $jar $algos $caseIDs $heatmapType $input $output $inputdb $inputcollection $outputdb $outputcollection $resultExeID"

$SPARK_HOME/bin/spark-submit --class $className $jar $algos $caseIDs $heatmapType $input $output $inputdb $inputcollection $outputdb $outputcollection $resultExeID
