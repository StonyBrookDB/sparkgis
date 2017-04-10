#!/bin/bash

# Package versions used in this script (latest at the time of writing)
# OS:     CentOS 7
# Java:   openjdk-8-jdk
# Spark:  2.1.0 (Standalone)
# Hadoop: 2.7.3
# 

# Setup ssh
yum install openssh-server
# setup Java
yum install java-1.8.0-openjdk-devel
# setup required packages
yum install gcc
yum install wget
