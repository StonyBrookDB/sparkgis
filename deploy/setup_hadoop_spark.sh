#!/bin/bash

# Package versions used in this script (latest at the time of writing)
# OS:     ubuntu-server 16.04.2 (LTS)
# Java:   openjdk-8-jdk
# Spark:  2.1.0 (Standalone)
# Hadoop: 2.7.3
# 

# Setup hadoop
echo "$(tput setaf 2)Setting up Hadoop ...$(tput sgr 0)"
wget http://download.nextag.com/apache/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
tar -xzf hadoop-2.7.3.tar.gz
sudo mv hadoop-2.7.3 /usr/local/hadoop
sudo mkdir /usr/local/hadoop/logs
sudo chmod -R 777 /usr/local/hadoop/logs
sudo mkdir /usr/local/hadoop/tmp
sudo chmod -R 777 /usr/local/hadoop/tmp
echo "$(tput setaf 2)Hadoop set up successfully: $(tput setaf 5)/usr/local/hadoop $(tput sgr 0)"
echo "$(tput setaf 1)$(tput setab 7)Setting up HDFS requires manual configuration. Please refer to $(tput setaf 5)configure_hdfs_local.sh$(tput setaf 1) for details $(tput sgr 0)"

# Setup Spark
echo "$(tput setaf 2)Setting up Spark ...$(tput sgr 0)"
wget http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz
tar -xzf spark-2.1.0-bin-hadoop2.7.tgz
sudo mv spark-2.1.0-bin-hadoop2.7 /usr/local/spark
echo "$(tput setaf 2)Spark set up successfully: $(tput setaf 5)/usr/local/spark $(tput sgr 0)"
echo "$(tput setaf 1)$(tput setab 7)Setting up Spark requires manual configuration. Please refer to $(tput setaf 5)configure_spark.sh$(tput setaf 1) for details $(tput sgr 0)"

# setup environment variables
echo "$(tput setaf 2)Setting up environment variables ...$(tput sgr 0)"
echo "# SparkGIS " >> ~/.bashrc
echo export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::") >> ~/.bashrc
echo export SPARK_HOME=/usr/local/spark >> ~/.bashrc
echo export HADOOP_HOME=/usr/local/hadoop >> ~/.bashrc

source ~/.bashrc

# alias for convenience
echo "alias spark-start-all='${SPARK_HOME}/sbin/start-all.sh'">>~/.bashrc
echo "alias spark-stop-all='${SPARK_HOME}/sbin/stop-all.sh'">>~/.bashrc

echo "alias hadoop-start-all='${HADOOP_HOME}/sbin/start-all.sh'">>~/.bashrc
echo "alias hadoop-stop-all='${HADOOP_HOME}/sbin/stop-all.sh'">>~/.bashrc

echo "alias hcat='hdfs dfs -cat'" >> ~/.bashrc
echo "alias hls='hdfs dfs -ls -h'" >> ~/.bashrc
echo "alias hput='hdfs dfs -put'" >> ~/.bashrc
echo "alias hget='hdfs dfs -get'" >> ~/.bashrc
echo "alias hrm='hdfs dfs -rm'" >> ~/.bashrc

source ~/.bashrc

echo "export PATH=\${HADOOP_HOME}/bin:\${SPARK_HOME}/bin:$PATH">>~/.bashrc

# cleanup
rm spark-2.1.0-bin-hadoop2.7.tgz
rm hadoop-2.7.3.tar.gz
