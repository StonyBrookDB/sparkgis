# SparkGIS Environment Setup

## List of dependencies
* gcc
* g++
* cmake
* java
* ssh
* geos
* libspatialindex
* maven (if compiling from source)

## Main scripts (Should be executed on each node)
* `rhel_setup_dependencies.sh` setup dependencies on RHEL. (requires root permissions)
* `ubuntu_setup_dependencies.sh` setup dependencies on Ubuntu. (requires root permissions)
* `eagle_setup_dependencies.sh` setup dependencies on StonyBrook BMI cluster.
* `setup_spatial_libs_from_source.sh` downloads and setup `geos` and `libspatialindex` in `../deps/`. This should only be executed directly in case root privileges are not available. However, first 4 of the dependencies listed above needs to be available to execute this script successfully.

## Optional scripts
* `setup_hadoop_spark.sh` downloads hadoop (ver 2.7.3) spark (ver 2.1.0) and sets them up in `/usr/local/`. Sets appropriate environment variables and helpful alias'es as well. 

## Helper scripts
* `configure_spark.sh` does not contain any code to execute. However, it lists down the steps required to configure spark to work with SparkGIS. (IMPORTANT STEP)
* `configure_hdfs_local.sh` does not contain any code to execute. However, it lists down the steps required to setup HDFS locally on a single node.
