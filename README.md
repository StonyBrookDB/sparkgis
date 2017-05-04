# SparkGIS

## Setup and Installation
The following steps needs to be followed in order to compile and run SparkGIS from source
### Setting up depenedencies
* Based on user's operating system and available privileges, appropriate script needs to be executed in `deploy/` directory. For instance, if target operating system is Ubuntu-Server and user has root privileges, following set of commands needs to be executed
```bash
cd deploy
sh ubuntu_setup_dependencies.sh
```
* Similarly for RHEL with root permissions
```bash
cd deploy
sh ubuntu_setup_dependencies.sh
```
* In case user does not have root permissions, he has to have the following syetem packages available before he can setup dependencies. After that the following script can be executed to setup required spatial libraries 
  * gcc
  * g++
  * cmake
```bash
cd deploy
sh setup_spatial_libs_from_source.sh
```
### Compiling from source
* Once dependencies are setup, following compile script can be executed which will compile native libraries as well as the Java code using `maven`
```bash
sh compile.sh
```
### Linking with Apache Spark
* The `lib/` directory needs to be available to spark worker nodes in order to process spatial queries. This can be done by setting following Spark properies either in `$SPARK_HOME/conf/spark-default-conf`, while creating `SparkConf` object or when submitting the job to spark through commandline
  * `spark.driver.extraLibraryPath`
  * `spark.executor.extraLibraryPath`

## Running sample job (Single node)
Current project comes with a sample data to test SparkGIS. After setting up environment and installation as mentioned in the above section, executing following set of commands will run a sample spatial join query on sample dataset and generate a per tile heatmap. This assumes that you have already setup and running HDFS, Spark and SparkGIS on your setup.
### Prepare input datasets
```bash
cd deploy
hdfs dfs -mkdir -p /sparkgis/sample_data/algo-v1
hdfs dfs -mkdir /sparkgis/sample_data/algo-v2
hdfs dfs -put sample_pia_data/Algo1-TCGA-02-0007-01Z-00-DX1 /sparkgis/sample_data/algo-v1/TCGA-02-0007-01Z-00-DX1
hdfs dfs -put sample_pia_data/Algo2-TCGA-02-0007-01Z-00-DX1 /sparkgis/sample_data/algo-v2/TCGA-02-0007-01Z-00-DX1
cd ..
```
### Modify SparkGIS properties
* Update following variables in `conf/sparkgis.properies`
  * `hdfs-algo-data=/sparkgis/sample_data/` 
  * `hdfs-hm-results=/sparkgis/sample_results/`
* Update heatmap script in `scripts/generate_heatmap.sh`
  * algos=' --algos "algo-v1,algo-v2"'
  * caseIDs=' --caseids "TCGA-02-0007-01Z-00-DX1"'

### Execute SparkGIS
```bash
cd scripts
sh generate_heatmap.sh
```
This should generate heatmap results in `hdfs://127.0.0.1:54310:/sparkgis/sample_results` directory.

## Directory Structure
* conf/
  * Contains customizable properties for SparkGIS
* deploy/
  * Contains deployment scripts for Ubuntu-server and CentOS
  * Other OSes coming soon ...
*deps/
  * Contains compiled spatial libraries i.e. `geos` and `libspatialindex` in case root privileges are not available. 
  * Environment variables `SPGIS_INC_PATH` and `SPGIS_LIB_PATH` should point to `deps/include` and `deps/lib` in case spatial libraries are compiled here. Otherwise, if spatial libraries are setup with default settings, `SPGIS_INC_PATH` and `SPGIS_LIB_PATH` should point to `/usr/include` and `/usr/lib` repectively.
* lib/
  * Contains compiled shared library for native code 
  * `spark.driver.extraLibraryPath` and `spark.executor.extraLibraryPath` should point to this folder. (Can be done in `$SPARK_HOME/conf/spark-default.con` or at runtime through `SparkConf` or through commandline while submitting job)
* scripts/
  * Shell scripts for various jobs
* src/
  * Source code for SparkGIS
  * `src/main/java/sparkgis/*` contains all Java source code
  * `src/main/java/jni/*` contains JNI interface and native c/c++ code
