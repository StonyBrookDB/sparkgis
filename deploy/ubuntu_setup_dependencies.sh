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

# Setup Java
echo "$(tput setaf 2)Setting up Java ...$(tput sgr 0)"
sudo apt-get install openjdk-8-jdk
echo "$(tput setaf 2)Done ... $(tput sgr 0)"

# setup dependencies GEOS & SpatialIndex
sudo apt-get install libgeos++-dev
sudo apt-get install libspatialindex-dev

echo "export SPGIS_INC_PATH=/usr/include">>~/.bashrc
echo "export SPGIS_LIB_PATH=/usr/lib">>~/.bashrc
echo "LD_CONFIG_PATH=\${LD_LIBRARY_PATH}:\${SPGIS_LIB_PATH}">>~/.bashrc
echo "export LD_LIBRARY_PATH=\${LD_CONFIG_PATH}">>~/.bashrc