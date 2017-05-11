package sparkgis.pia;
/* Java imports */
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
/* Local imports */
import jni.JNIWrapper;
import sparkgis.data.Tile;
import sparkgis.enums.HMType;
import sparkgis.data.TileStats;
import sparkgis.data.Space;
import sparkgis.data.BinaryDataConfig;
import sparkgis.enums.Predicate;
import sparkgis.data.SpatialObject;
import sparkgis.core.ASpatialJoin;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.coordinator.SparkGISJobConf;
import sparkgis.core.partitioning.Partitioner;
import sparkgis.core.spatialindex.SparkSpatialIndex;

/**
 * Spark Spatial Join for HeatMap Generation
 */
public class SparkSpatialJoinHMBinary implements Serializable{

    private final SparkGISJobConf sgjConf;
    private final HMType hmType;
    private final Predicate predicate;
    private final BinaryDataConfig config1;
    private final BinaryDataConfig config2;
    private final Space combinedSpace;

    /* 
     * Combined configuration values removed from DataConfig 
     * No need to inflate DataConfig object 
     * since it has to be transferred over to workers
     */
    private Broadcast<SparkSpatialIndex> ssidxBV = null;
    
    public SparkSpatialJoinHMBinary(
				    SparkGISJobConf sgjConf,
				    BinaryDataConfig config1,
				    BinaryDataConfig config2,
				    Predicate predicate,
				    HMType hmType
				  ){
	this.sgjConf = sgjConf;
	this.predicate = predicate;
	this.hmType = hmType; 
	this.config1 = config1;
	this.config2 = config2;

	combinedSpace = new Space();
	/* set combined data configuration */
	final double minX = (config1.space.getMinX() < config2.space.getMinX())?config1.space.getMinX():config2.space.getMinX();
	final double minY = (config1.space.getMinY() < config2.space.getMinY())?config1.space.getMinY():config2.space.getMinY();
	final double maxX = (config1.space.getMaxX() > config2.space.getMaxX())?config1.space.getMaxX():config2.space.getMaxX();
	final double maxY = (config1.space.getMaxY() > config2.space.getMaxY())?config1.space.getMaxY():config2.space.getMaxY();

	combinedSpace.setMinX(minX);
	combinedSpace.setMinY(minY);
	combinedSpace.setMaxX(maxX);
	combinedSpace.setMaxY(maxY);
	
	combinedSpace.setSpaceObjects(config1.space.getSpaceObjects() + config2.space.getSpaceObjects());

    }
    
    public JavaRDD<TileStats> execute(){
	
	List<Tile> partitionIDX =
	    /* DONOT DENORMALIZE IF USING 'HM' PARTITIONER */
	    Partitioner.fixedGridHM(
	    			    combinedSpace.getMinX(), 
	    			    combinedSpace.getMinY(), 
	    			    combinedSpace.getMaxX(),
	    			    combinedSpace.getMaxY(),
	    			    this.sgjConf.getPartitionSize()
	    			    );
	/* 
	 * Broadcast ssidx 
	 * ssidx is not very big, will this help???
	 */
    	final SparkSpatialIndex ssidx = new SparkSpatialIndex();
    	ssidx.build(partitionIDX);
	ssidxBV = SparkGISContext.sparkContext.broadcast(ssidx);

	JavaPairRDD<Integer, Tuple2<Iterable<byte[]>,Iterable<byte[]>>>
	    groupedMapData = getDataByTile();


	System.out.println("Tiles: " + partitionIDX.size());
	System.out.println("Config1: " + config1.getData().count());
	System.out.println("Config2: " + config2.getData().count());
	System.out.println("Binary Count: " + groupedMapData.count());
	
	return SparkGISContext.sparkContext.emptyRDD();
	
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

    	// /* Call Jaccard function to calculate jaccard coefficients per tile */
    	// return Coefficient.execute(
    	// 			   results.values(),
    	// 			   /*spJoinResult,*/ 
    	// 			   partitionIDX,
    	// 			   hmType
    	// 			   );
	// }
	
    }

    /*
     * Cogroup version
     */
    protected JavaPairRDD<Integer, Tuple2<Iterable<byte[]>, Iterable<byte[]>>> getDataByTile(){

	/* 
	 * Reformat stage only appends a set number to data from algo1 and algo2 
	 * It has been merged with Partition mapper join stage
	 */
	
    	JavaPairRDD<Integer, byte[]> joinMapData1 = 
    	    config1.getData().flatMapToPair(new PartitionMapperJoin());

	JavaPairRDD<Integer, byte[]> joinMapData2 = 
    	    config2.getData().flatMapToPair(new PartitionMapperJoin());

	JavaPairRDD<Integer, Tuple2<Iterable<byte[]>, Iterable<byte[]>>> groupedData =
	    joinMapData1.cogroup(joinMapData2);

	return groupedData;
    }

    /**
     * Called for all data corresponding to a given key after groupByKey()
     */
    class Resque
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
    	    String[] results = jni.resqueSPJ(
    					  data.toArray(dataArray),
    					  predicate,
    					  geomid1,
    					  geomid2
    					  );
    	    //for (String res : results)
	    //ret.add(res);
    	    return Arrays.asList(results);
    	}
    }
    
    /**
     * Replicated code. Fixed in newer version
     */
    class ResqueTileDice implements Function<Iterable<String>, Double>{
    	private final int predicate;
    	private final int geomid1;
    	private final int geomid2;
    	public ResqueTileDice(int predicate, int geomid1, int geomid2){
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
    	    double result = jni.resqueTileDice(
					data.toArray(dataArray),
					predicate,
					geomid1,
					geomid2
					);
    	    return new Double(result);
    	}
    }
    
    /**
     * Maps each spatialObject to a tile from spatial index after appending a set number to the data.
     * NOTE: There is a difference between joinIDX and setNumber
     * In case of any issue, please refer to Journal Entry: Jan 24, 2017
     * @param setNumber Dataset this spatialObject belongs to
     * @return tileID,binarySpatialObject
     //@return tileID,joinIDX,setNumber,id,spatialObject
     */
    class PartitionMapperJoin implements PairFlatMapFunction<byte[], Integer, byte[]>{
	// private final int setNumber;

	// public PartitionMapperJoin(int setNumber){
	//     this.setNumber = setNumber;
    	// }
    	public Iterator<Tuple2<Integer, byte[]>> call (final byte[] binaryPolygon){

	    /* get spatial index from braodcast variable */
	    final SparkSpatialIndex ssidx = ssidxBV.value();
	    // final int joinIDX = (setNumber==1)? 2 : 1;
	    
    	    List<Tuple2<Integer, byte[]>> ret = new ArrayList<Tuple2<Integer, byte[]>>();
	    List<Long> tileIDs = ssidx.getIntersectingIndexTiles(binaryPolygon);
    	    for (long id : tileIDs){
		//String retLine = id + "\t" + joinIDX + "\t" + this.setNumber + "\t" + s.toString();
    		Tuple2<Integer, byte[]> t = new Tuple2<Integer, byte[]>((int)id, binaryPolygon);
    		ret.add(t);
    	    }
    	    return ret.iterator();
    	}
    }
}
