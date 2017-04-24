#!/bin/bash

# Package versions used in this script (latest at the time of writing)
# OS:     ubuntu-server 16.04.2 (LTS)
# Java:   openjdk-8-jdk
# Spark:  2.1.0 (Standalone)
# Hadoop: 2.7.3
# 

# Setup ssh
echo "$(tput setaf 2)Setting up ssh ...$(tput sgr 0)"
sudo apt-get install openssh-server
ssh-keygen
ssh-copy-id -i ~/.ssh/id_rsa.pub localhost
echo "$(tput setaf 2)Done ... $(tput sgr 0)"

# setup dependencies GEOS & SpatialIndex
sudo apt-get install libgeos++-dev
sudo apt-get install libspatialindex-dev

# Setup Java
echo "$(tput setaf 2)Setting up Java ...$(tput sgr 0)"
sudo apt-get install openjdk-8-jdk
echo "$(tput setaf 2)Done ... $(tput sgr 0)"

# Setup hadoop
echo "$(tput setaf 2)Setting up Hadoop ...$(tput sgr 0)"
wget http://download.nextag.com/apache/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
tar -xzf hadoop-2.7.3.tar.gz
sudo mv hadoop-2.7.3 /usr/local/hadoop
# sudo install -d -o sparkgis -g sparkgis -m 700 /usr/local/hadoop/tmp
# sudo install -d -o sparkgis -g sparkgis -m 700 /usr/local/hadoop/logs
sudo mkdir /usr/local/hadoop/logs
sudo chmod -R 700 /usr/local/hadoop/logs
sudo mkdir /usr/local/hadoop/tmp
sudo chmod -R 700 /usr/local/hadoop/tmp
echo "$(tput setaf 2)Hadoop set up successfully: $(tput setaf 5)/usr/local/hadoop $(tput sgr 0)"
echo "$(tput setaf 1)$(tput setab 7)Setting up HDFS requires manual configuration. Please refer to $(tput setaf 5)setup_hdfs.sh$(tput setaf 1) for details $(tput sgr 0)"

# Setup Spark
echo "$(tput setaf 2)Setting up Spark ...$(tput sgr 0)"
wget http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz
tar -xzf spark-2.1.0-bin-hadoop2.7.tgz
sudo mv spark-2.1.0-bin-hadoop2.7 /usr/local/spark
echo "$(tput setaf 2)Spark set up successfully: $(tput setaf 5)/usr/local/spark $(tput sgr 0)"

# Setup SparkGIS

# setup environment variables
echo "$(tput setaf 2)Setting up environment variables ...$(tput sgr 0)"
echo "# SparkGIS " >> ~/.bashrc
echo export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::") >> ~/.bashrc
echo export SPARK_HOME=/usr/local/spark >> ~/.bashrc
echo export HADOOP_HOME=/usr/local/hadoop >> ~/.bashrc

# alias for convenience
echo alias spark-start-all=\'${SPARK_HOME}/sbin/start-all.sh\' >> ~/.bashrc
echo alias spark-stop-all=\'${SPARK_HOME}/sbin/stop-all.sh\' >> ~/.bashrc

echo alias hadoop-start-all=\'${HADOOP_HOME}/sbin/start-all.sh\' >> ~/.bashrc
echo alias hadoop-stop-all=\'${HADOOP_HOME}/sbin/stop-all.sh\' >> ~/.bashrc

echo alias hcat=\'hdfs dfs -cat\' >> ~/.bashrc
echo alias hls=\'hdfs dfs -ls -h\' >> ~/.bashrc
echo alias hput=\'hdfs dfs -put\' >> ~/.bashrc
echo alias hget=\'hdfs dfs -get\' >> ~/.bashrc
echo alias hrm=\'hdfs dfs -rm\' >> ~/.bashrc

echo export PATH=$PATH:${SPARK_HOME}/bin:${HADOOP_HOME}/bin:${PATH} >> ~/.bashrc 
source ~/.bashrc

# # start hdfs
echo "$(tput setaf 2)Starting HDFS $(tput sgr 0)"
# hadoop-start-all

# cleanup
rm spark-2.1.0-bin-hadoop2.7.tgz
rm hadoop-2.7.3.tar.gz
