#!/bin/bash

module load gcc
module load bmi-hadoop/hadoop/2.4.1
module load cmake30/3.0.0
module load maven/3.3.3
module load jdk8/1.8.0_66
# should be modified to a generic version - but should work for now
echo "export HADOOP_CONF_DIR=/home/hoang/nfsconfig">>~/.bashrc
# setup spatial libraries from source since we don't have root access
sh setup_spatial_libs_from_source.sh
