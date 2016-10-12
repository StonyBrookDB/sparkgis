#!/bin/bash

USER=fbaig
HADOOP_VER=2.7.3
SPARK_VER=1.6.1-bin-hadoop2.6

# setup java
sudo apt-get install openjdk-8-jdk
# setup ssh
sudo apt-get install openssh

####################### setup HDFS ####################
# Reference: http://askubuntu.com/questions/144433/how-to-install-hadoop
# download hadoop
wget http://www.trieuvan.com/apache/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz
tar -xvzf hadoop-$HADOOP_VER.tar.gz
mv hadoop-$HADOOP_VER hadoop

# set enviroment variable
export HADOOP_HOME=/home/$USER/hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# update hadoop scripts
#    hadoop-env.sh - set JAVA_HOME 
#    core-site.xml - set hadoop.tmp.dir (tmp folder) & fs.default.name (file system name)
#    hdfs-site.xml - set dfs.replication

# format namenode
hdfs namenode -format
# star hadoop service
start-dfs.sh

###################### setup Spark ####################
wget http://d3kbcqa49mib13.cloudfront.net/spark-$SPARK_VER.tgz
tar -xvzf spark-$SPARK_VER.tgz
mv spark-$SPARK_VER spark

#################### setup SparkGIS ###################
# Manage Dependencies
# libspatialIndex
# sudo apt-get install build-essential libtool
# sudo apt-get install autotools-dev
# sudo apt-get install automake
# git clone https://github.com/libspatialindex/libspatialindex.git
# cd libspatialindex
# ./autogen.sh
# ./configure
sudo apt-get install libspatialindex-dev
# geos
sudo apt-get install libgeos++-dev
# install maven
sudo apt-get install maven
# compile SparkGIS
git clone https://github.com/SBU-BMI/spark-gis.git
cd sparkgis/heatmapssparkgis-prod
sh compile.sh
