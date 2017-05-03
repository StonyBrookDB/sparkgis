#!/bin/bash

# configure hadoop (Local single node setup)
#
# (1) set JAVA_HOME explicitly in /usr/local/hadoop/etc/hadoop/hadoop-env.sh
#
# (2) /usr/local/hadoop/etc/hadoop/hdfs-site.xml
#  <property>
#    <name>dfs.replication</name>
#    <value>1</value>
#  </property>
#
# (3) /usr/local/hadoop/etc/hadoop/core-site.xml
# <property>
#    <name>hadoop.tmp.dir</name>
#    <value>/usr/local/hadoop/tmp</value>
# </property>
# <property>
#   <name>fs.default.name</name>
#   <value>hdfs://localhost:54310</value>
# </property>
#
# (4) /usr/local/hadoop/etc/hadoop/mapred-site.xml
# <property>
#    <name>mapred.job.tracker</name>
#    <value>localhost:54311</value>
# </property>
#
# (5) create hdfs tmp folder
# mkdir -p /usr/local/hadoop/tmp
# (6) format hdfs namenode
# hdfs namenode -format
