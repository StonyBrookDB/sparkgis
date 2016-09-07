#!/bin/bash
# include path for spatial index 
#SPINDEX_INC="extlib/include"
#SPINDEX_LIB="extlib/lib"
# to compile with C++11
#COMP_FLAG="-std=c++0x"

cd ..
javac -d ./jni jni/JNIWrapper.java
javah -jni -d ./jni jni.JNIWrapper
cd -
make
#g++ $COMP_FLAG -shared -fpic -o libgis.so -I/usr/lib/jvm/java-7-openjdk-amd64/include -I/usr/lib/jvm/java-7-openjdk-amd64/include/linux gis.cpp -I$SPINDEX_INC -lspatialindex -lgeos -L$SPINDEX_LIB native/partitionMapperJoin.cpp

