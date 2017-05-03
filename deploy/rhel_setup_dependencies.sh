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
yum install gcc
yum install g++
# Setup cmake
yum install cmake
# Setup ssh
yum install openssh-server
# setup Java
yum install java-1.8.0-openjdk-devel
yum install wget
# setup dependencies GEOS & SpatialIndex from source since they are not available through yum
sh setup_spatial_libs_from_source.sh
