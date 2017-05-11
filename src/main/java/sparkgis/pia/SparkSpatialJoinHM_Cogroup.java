package sparkgis.pia;
/* Java imports */
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
/* Local imports */
import sparkgis.enums.HMType;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.core.ASpatialJoin;
import sparkgis.core.SparkSpatialJoin;
import sparkgis.coordinator.SparkGISJobConf;

/**
 * Spark Spatial Join for HeatMap Generation
 */
public class SparkSpatialJoinHM_Cogroup extends ASpatialJoin<TileStats> implements Serializable{

    private final HMType hmType;
    
    public SparkSpatialJoinHM_Cogroup(SparkGISJobConf sgjConf,
				      DataConfig config1,
				      DataConfig config2,
				      Predicate predicate,
				      HMType hmType
				      ){
	super(sgjConf, config1, config2, predicate);
	this.hmType = hmType; 
    }
    
    public JavaRDD<TileStats> execute(){

	/* Spatial join results */
	JavaRDD<Iterable<String>> results = (new SparkSpatialJoin(sgjConf, config1, config2, predicate)).execute();
	/* Call function to calculate similarity coefficients per tile */
	JavaRDD<TileStats> stats = Coefficient.execute(
    				   results,
    				   partitionIDX,
    				   hmType
    				   );
    	return stats;
	
	// /* Native C++: Resque */
	// if (hmType == HMType.TILEDICE){
	//     throw new java.lang.RuntimeException("Not implemented in Cogroup version yet");
	//     // JavaPairRDD<Integer, Double> tileDiceResults = 
	//     // 	groupedMapData.mapValues(new ResqueTileDice(
	//     // 						    predicate.value,
	//     // 						    config1.getGeomid(),
	//     // 						    config2.getGeomid()
	//     // 						    )
	//     // 				 ).filter(new Function<Tuple2<Integer, Double>, Boolean>(){
	//     // 					 public Boolean call(Tuple2<Integer, Double> t){
	//     // 					     if (t._2() == -1)
	//     // 						 return false;
	//     // 					     return true;
	//     // 					 }
	//     // 				     });
	//     // return Coefficient.mapResultsToTile(
	//     // 					this.partitionIDX, 
	//     // 					tileDiceResults,
	//     // 					hmType
	//     // 					);
	// }
	// else{
	//     JavaPairRDD<Integer, Iterable<String>> results = 
	// 	groupedMapData.mapValues(new Resque(
    	// 				      predicate.value, 
    	// 				      config1.getGeomid(),
    	// 				      config2.getGeomid())
	// 			     );	
    	// JavaRDD<Iterable<String>> vals = results.values();
	
    }
}
