
#!/bin/bash
# hardcoded values for debugging purposes
className=driver.DataCubeMain
jar=target/uber-spark-gis-1.0.jar

# run spark job
/home/cochung/spark-1.6.2-bin-hadoop2.6/bin/spark-submit --class $className $jar

 




