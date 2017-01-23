# SparkGIS
(General SparkGIS intro goes here)

Latest merge from RawDataProcessing branch. Latest updates are as follows
* No `KryoSerializer` used anymore
* Code for final heatmap stage (per tile stats calculation) uses Broadcast Variable joining (needs working ...)
* `HeatMapTask` now uses `SpatialJoinHM_Cogroup` instead of `SpatialJoinHM`
* Uses `cogroup` instead of `groupByKey` (cleaner code, mostly experimental but no considerable performance difference since a shuffle is still necessary to group per tile data)