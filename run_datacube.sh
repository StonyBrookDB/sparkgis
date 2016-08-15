
#!/bin/bash
# hardcoded values for debugging purposes
className=driver.DataCubeMain
jar=target/uber-spark-gis-1.0.jar
caseIDs=`echo $1 | tr -d "'"`
inputdb=`echo $2 | tr -d "'"`
inputcollection=`echo $3 | tr -d "'"`
dimension=`echo $4 | tr -d "'"`
# run spark job
/home/cochung/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class $className $jar $caseIDs $inputdb $inputcollection $dimension
 # sh run_datacube.sh ' --caseids "TCGA-CS-4941-01Z-00-DX1"' ' --inputdb "u24_nodejs"' ' --inputcollection "objects"'
#sh run_datacube.sh ' --caseids "TCGA-CS-4941-01Z-00-DX1"' ' --inputdb "temp_db2"' ' --inputcollection "temp_col2"'


# sh run_datacube.sh ' --caseids "TCGA-CS-4941-01Z-00-DX1"' ' --inputdb "temp_db2"' ' --inputcollection "temp_col2"' ' --dimension "Area_100,Perimeter_200,Elongation_300"' 