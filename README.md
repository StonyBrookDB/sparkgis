# Py-SparkGIS

SparkGIS docker implementation in python with compatability updates for latest Spark version 3.1.1. The basic goal of this repository is to make SparkGIS reproducable and portable. This branch can also serve as basis for further improvements. For instance, while `python` based implementations require less time to develop, there are known performance issues since python is a JVM supported language. JVM supported languages such as Java and Scala are reported to perform upto 10x better as compared to python for Spark.

# How to Run?

- `cd docker/`
- `sh docker_run.sh`
  - This will build `sparkgis` docker image and create `sparkgis` container
  - Once built, it will give a terminal to the `sparkgis` container
  - Alternatively, you can get into the running container using `docker exec -it sparkgis bash`
- Once inside the container, make sure you are in `/code/` directory
- Run `sh run.sh` to run a sample `SpatialJoin` operation on the sample dataset
- If all goes well, an output directory will be created `/data/out` with intersecting polygon pairs from sample datasets
- The output directory will also contain a sample validation plot for top 100 intersecting polygons

## Optional

- In `docker/docker_run.sh` update `LOCAL_DATA_PATH` to point to local directory containing spatial data. This path will be mapped to `/data/` directory inside the docker container
- NOTE: As of now, the sample dataset contains about 300,000 polygons in total. Processing all of them in local environment can prove to be prohibitive. For debugging and testing `code/sparkgis_driver.py` limits data to `1000` polygons from each dataset only. For all practical purposes, this should be commented out. 

## Important!!!

- SparkGIS pipeline expects `DataFrame`s having at least two columns i.e. `gid` and `geometry`
- `partition_size` parameter in `FixedGridPartitioner()` directly effects performance

# Directory Structure

- `code/`: Core python code for SparkGIS
- `data/`: Sample data for testing
- `docker/`: Docker scripts and configurations to build and run SparkGIS in containerized environment 

# Some Additional Notes

- As of now, only Spatial Join is implemented, implement `SpatialRange`, `SpatialkNN` and `SpatialkNNJoin`in next version
- As of now, this only supports `FixedGridPartitioning`. Other strategies need to be implemented
- Add docker support for local testing i.e. build directly on spark docker image. Current docker image is ubuntu based
- Further improve documentation
- Seperate configuration from core