/**
 * PATHOLOGY IMAGE ANALYTICS PLUGIN
 */
package sparkgis.pia;
/* Java imports */
import java.util.ArrayList;
import java.util.Iterator;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
/* Local imports */
import jni.JNIWrapper;
import sparkgis.core.enums.Predicate;
import sparkgis.core.data.DataConfig;
import sparkgis.core.data.TileStats;
import sparkgis.core.executionlayer.SparkSpatialJoin;

/**
 * Spark Spatial Join for HeatMap Generation
 */
public class SparkSpatialJoinHM extends SparkSpatialJoin implements Serializable{
    
    public SparkSpatialJoinHM(DataConfig config1, DataConfig config2, Predicate predicate, int partitionSize){
	
	super(config1, config2, predicate, partitionSize);	
    }
    
    public JavaRDD<TileStats> execute(HMType hmType){
	
	/*
	 * TileDice is calculated natively by RESQUE
	 * Refer to JNIWrapper.java for documentation
	 */
	if (hmType == HMType.TILEDICE){
	    JavaPairRDD<Integer, Iterable<String>> groupedMapData = super.getDataGroupedByTile();
	    
	    JavaPairRDD<Integer, Double> tileDiceResults = 
		groupedMapData.mapValues(new ResqueTileDice(
							    predicate.value,
							    super.config1.getGeomid(),
							    super.config2.getGeomid()
							    )
					 ).filter(new Function<Tuple2<Integer, Double>, Boolean>(){
						 public Boolean call(Tuple2<Integer, Double> t){
						     if (t._2() == -1)
							 return false;
						     return true;
						 }
					     });
	    return Coefficient.mapResultsToTile(
						super.combinedConfig.getPartitionIDX(), 
						tileDiceResults,
						hmType
						);
	}
	else{
	    // Do a spatial join 
	    JavaPairRDD<Integer, Iterable<String>> joinRDD = super.execute();
	    // Calculate metric type statistics per tile to generate heatmap
	    return Coefficient.execute(
    				   joinRDD.values(),
    				   super.combinedConfig.getPartitionIDX(),
    				   hmType
    				   );
	}
    }

    class ResqueTileDice implements Function<Iterable<String>, Double>{
    	private final String predicate;
    	private final int geomid1;
    	private final int geomid2;
    	public ResqueTileDice(String predicate, int geomid1, int geomid2){
    	    this.predicate = predicate;
    	    this.geomid1 = geomid1;
    	    this.geomid2 = geomid2;
    	}
    	public Double call (final Iterable<String> inData){
    	    ArrayList<String> data = new ArrayList<String>();
    	    for (String in : inData)
    		data.add(in);	    
	    
    	    String[] dataArray = new String[data.size()];	    
    	    JNIWrapper jni = new JNIWrapper();
    	    double results = jni.resqueTileDice(
					data.toArray(dataArray),
					predicate,
					geomid1,
					geomid2
					);
    	    return new Double(results);
    	}
    }
}
