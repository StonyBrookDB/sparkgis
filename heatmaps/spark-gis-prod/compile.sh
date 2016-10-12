#!/bin/bash

# compile native library
cd src/main/java/jni/
sh compile.sh
mv libgis.so ../../../../lib/
cd -
# compile project using maven
mvn clean
mvn package
cd ../

# cd spark-gis-io/
# mvn clean
# mvn package
# cd ../
