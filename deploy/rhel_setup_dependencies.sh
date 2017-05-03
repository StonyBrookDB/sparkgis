#!/bin/bash

# Package versions used in this script (latest at the time of writing)
# OS:     CentOS 7
# Java:   openjdk-8-jdk
# Spark:  2.1.0 (Standalone)
# Hadoop: 2.7.3
# 

echo "****** REQUIRES ROOT PRIVILEGES ******"
echo "If you donot have root privileges, ask your admin to install gcc, g++, cmake and java. Run setup_spatial_libs_from_source.sh afterwards"

# Setup gcc & g++
sudo yum install gcc
sudo yum install gcc-c++
# Setup cmake
sudo yum install cmake
# Setup ssh
sudo yum install openssh-server
# setup Java
sudo yum install java-1.8.0-openjdk-devel
echo export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::") >> ~/.bashrc

sudo yum install wget
# setup maven
sudo yum install maven
# setup bzip2 require for unzipping source for spatial libraries
sudo yum install bzip2
# setup dependencies GEOS & SpatialIndex from source since they are not available through yum
sh setup_spatial_libs_from_source.sh
