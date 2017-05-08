#!/bin/bash

# compile native library
cd src/main/java/jni/
sh compile.sh
RET=$?
mv libgis.so ../../../../lib/
cd -
if [ $RET -eq 0 ]; then
    # compile project using maven
    mvn clean package
    cd ../
fi

