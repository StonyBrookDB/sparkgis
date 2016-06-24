#!/bin/bash

# compile project using maven
cd spark-gis/
mvn clean
mvn package
cd ../


# cd spark-gis-io/
# mvn clean
# mvn package
# cd ../