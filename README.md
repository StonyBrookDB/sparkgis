# SparkGIS

Folder Heirarchy
* src/
  * Source code for SparkGIS
  * `src/main/java/sparkgis/*` contains all Java source code
  * `src/main/java/jni/*` contains JNI interface and native c/c++ code
* conf/
  * Contains customizable properties for SparkGIS
* lib/
  * Contains compiled shared library for native code 
  * `spark.driver.extraLibraryPath` and `spark.executor.extraLibraryPath` should point to this folder. (Can be done in `$SPARK_HOME/conf/spark-default.con` or at runtime through `SparkConf` or through commandline while submitting job)
* deploy/
  * Contains deployment scripts for Ubuntu-server and CentOS
  * Other OSes coming soon ...