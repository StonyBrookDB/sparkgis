#!/bin/bash

# ****** REQUIRES ROOT PRIVILEGES ******
# List of dependencies
# gcc
# g++
# cmake
# openssh-server
# java
# geos
# libspatialindex
# maven (if compiling from source)


echo "****** REQUIRES ROOT PRIVILEGES ******"
echo "If you donot have root privileges, ask your admin to install gcc, g++, cmake and java. Run setup_spatial_libs_from_source.sh afterwards"

# Setup gcc & g++
sudo apt-get install gcc
sudo apt-get install g++

# Setup cmake
sudo apt-get install cmake

# Setup ssh
echo "$(tput setaf 2)Setting up ssh ...$(tput sgr 0)"
sudo apt-get install openssh-server
#ssh-keygen
#ssh-copy-id -i ~/.ssh/id_rsa.pub localhost
echo "$(tput setaf 2)Done ... $(tput sgr 0)"

# setup dependencies GEOS & SpatialIndex
sudo apt-get install libgeos++-dev
sudo apt-get install libspatialindex-dev

# Setup Java
echo "$(tput setaf 2)Setting up Java ...$(tput sgr 0)"
sudo apt-get install openjdk-8-jdk
echo "$(tput setaf 2)Done ... $(tput sgr 0)"
