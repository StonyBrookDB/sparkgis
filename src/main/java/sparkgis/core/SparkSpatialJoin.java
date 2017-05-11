package sparkgis.core;
/* Java imports */
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
/* Local imports */
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.enums.PartitionMethod;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.coordinator.SparkGISJobConf;
import sparkgis.core.partitioning.Partitioner;
import sparkgis.core.spatialindex.SparkSpatialIndex;

/**
 * Spark Spatial Join
 */
public class SparkSpatialJoin extends ASpatialJoin<Iterable<String>> implements Serializable{

    public SparkSpatialJoin(
			    SparkGISJobConf sgjConf,
			    DataConfig config1,
			    DataConfig config2,
			    Predicate predicate
			    ){
	super(sgjConf, config1, config2, predicate);
    }
    
    /**
     * Performs spatial join operation on data configurations specified in constructor
     * @return JavaRDD<String>. Each string contains information about resulting pair of
     * polygons with some basic stats such as area union, intersecting area etc. The format
     * is as follows
     * <p> 
     * polygon-id \t polygon-coordinates \t polygon-id \t polygon-coordinates \t tile-id \t Jaccard \t dice
     */
    public JavaRDD<Iterable<String>> execute(){

	/* 
	 * Broadcast ssidx 
	 * ssidx is not very big, will this help???
	 */
    	final SparkSpatialIndex ssidx = new SparkSpatialIndex();
    	ssidx.build(partitionIDX);
	ssidxBV = SparkGISContext.sparkContext.broadcast(ssidx);

	JavaPairRDD<Integer, Tuple2<Iterable<String>,Iterable<String>>>
	    groupedMapData = getDataByTile();

	JavaPairRDD<Integer, Iterable<String>> results = 
	    groupedMapData.mapValues(new Resque(
						predicate.value, 
						config1.getGeomid(),
						config2.getGeomid())
				     );	
	// /* Native C++: Resque */
	//     JavaPairRDD<Integer, String> results = 
	// 	groupedMapData.flatMapValues(new Resque(
	// 						predicate.value, 
	// 						config1.getGeomid(),
	// 						config2.getGeomid())
	// 				     );
	return results.values();
    }
}
