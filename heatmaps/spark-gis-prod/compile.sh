#!/bin/bash

# UNCOMMENT FOR DEPLOYMENT
# compile native library
# cd src/main/java/jni/
# sh compile.sh
# mv libgis.so ../../../../lib/
# cd -
# compile project using maven
mvn clean package
cd ../

# cd spark-gis-io/
# mvn clean
# mvn package
# cd ../
