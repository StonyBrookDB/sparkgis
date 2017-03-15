# SparkGIS - partition branch

Main goals of this branch
* Add support for new partition algorithms other than just fixed grid
* Experiment with new query pipeline
  * Do partitioning ahead of time once only (similar to indexing)
  * Use the index to read respective tile data on each worker (avoid data shuffling)
  * Rest of the pipeline mostly remains unchanged
  * Adapt partitioning for different query types