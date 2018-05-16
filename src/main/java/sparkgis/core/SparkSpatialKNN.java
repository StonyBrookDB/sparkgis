package sparkgis.core;
/* Java imports */
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
/* Local imports */
import org.apache.spark.api.java.function.Function;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.enums.PartitionMethod;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.coordinator.SparkGISJobConf;
import sparkgis.core.partitioning.Partitioner;
import sparkgis.core.spatialindex.SparkSpatialIndex;
import jni.JNIWrapper;

/**
 * Spark Spatial KNN
 */
public class SparkSpatialKNN extends ASpatialQuery<Iterable<String>> implements Serializable{

    public SparkSpatialKNN(
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
	// 			
	//     );i
	System.out.println("Inside execute function of SparkSpatialKNN");

//	for (Iterable<String> is : results.values().collect()){
//		for (String s : is)
//			System.out.println(s);
//	}	
//
//	System.out.println(results.values());
	return results.values();
    }

    protected class Resque
	implements Function<Tuple2<Iterable<String>,Iterable<String>>, Iterable<String>>{

	private final int predicate;
    	private final int geomid1;
    	private final int geomid2;
    	public Resque(int predicate, int geomid1, int geomid2){
    	    this.predicate = predicate;
    	    this.geomid1 = geomid1;
    	    this.geomid2 = geomid2;
    	}
    	public Iterable<String> call (final Tuple2<Iterable<String>,Iterable<String>> inData){
    	    //List<String> ret = new ArrayList<String>();
    	    ArrayList<String> data = new ArrayList<String>();
    	    for (String in : inData._1())
    		data.add(in);
	    for (String in : inData._2())
    		data.add(in);
	    
    	    String[] dataArray = new String[data.size()];	    
    	    JNIWrapper jni = new JNIWrapper();
    	    String[] results = jni.resqueKNN(
    					  data.toArray(dataArray),
    					  predicate,5,
    					  geomid1,
    					  geomid2
    					  );
    	    //for (String res : results)
	    //ret.add(res);
	  //  System.out.println("Hello");
	    for(String s:results)
		System.out.println(s);
	    //System.out.println("World");
    	    return Arrays.asList(results);
    	}
    }
 

}
