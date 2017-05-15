package sparkgis.core;
/* Java imports */
import java.util.List;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
/* Local imports */
import sparkgis.data.Tile;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.data.SpatialObject;
import sparkgis.enums.PartitionMethod;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.coordinator.SparkGISJobConf;
import sparkgis.core.partitioning.Partitioner;
import sparkgis.core.spatialindex.SparkSpatialIndex;

/**
 * Spark Spatial Join
 * T Input spatial data type (SpatialObject OR byte[])
 */
public class SparkSpatialJoin <T>
    extends ASpatialQuery<T, Iterable<String>> 
    implements Serializable
{

    public SparkSpatialJoin(
			    SparkGISJobConf sgjConf,
			    DataConfig<T> config1,
			    DataConfig<T> config2,
			    Predicate predicate
			    ){
	super(sgjConf, config1, config2, predicate);
    }
    public SparkSpatialJoin(
			    SparkGISJobConf sgjConf,
			    DataConfig<T> config1,
			    DataConfig<T> config2,
			    Predicate predicate,
			    List<Tile> partitionIDX
			    ){
	super(sgjConf, config1, config2, predicate, partitionIDX);
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

	JavaPairRDD<Integer, Tuple2<Iterable<T>,Iterable<T>>>
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
