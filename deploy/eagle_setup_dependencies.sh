#!/bin/bash

echo "module load gcc">>~/.bashrc
echo "module load bmi-hadoop/hadoop/2.4.1">>~/.bashrc
echo "module load cmake30/3.0.0">>~/.bashrc
echo "module load maven/3.3.3">>~/.bashrc
echo "module load jdk8/1.8.0_66">>~/.bashrc
# should be modified to a generic version - but should work for now
echo "export HADOOP_CONF_DIR=/home/hoang/nfsconfig">>~/.bashrc

# setup spatial libraries from source since we don't have root access
sh setup_spatial_libs_from_source.sh
