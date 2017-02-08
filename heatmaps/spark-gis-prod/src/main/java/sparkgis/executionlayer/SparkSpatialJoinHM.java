package sparkgis.executionlayer;
/* Java imports */
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
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
import sparkgis.SparkGIS;
import sparkgis.data.Polygon;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.enums.HMType;
import sparkgis.stats.Profile;
import sparkgis.executionlayer.partitioning.Partitioner;
import sparkgis.executionlayer.spatialindex.SparkSpatialIndex;

/**
 * Spark Spatial Join for HeatMap Generation
 */
public class SparkSpatialJoinHM implements Serializable{
    
    private final Predicate predicate;
    private final HMType hmType;
    /* combined data configuration */
    private final DataConfig combinedConfig;
    private final DataConfig config1;
    private final DataConfig config2;

    private final double minX;
    private final double minY;
    private final double maxX;
    private final double maxY;

    /* 
     * Combined configuration values removed from DataConfig 
     * No need to inflate DataConfig object 
     * since it has to be transferred over to workers
     */
    private final int partitionSize;
    private List<Tile> partitionIDX;
    
    public SparkSpatialJoinHM(DataConfig config1,
			      DataConfig config2,
			      Predicate predicate,
			      HMType hmType,
			      int partitionSize){
	this.predicate = predicate;
	this.hmType = hmType; 
	this.config1 = config1;
	this.config2 = config2;
	combinedConfig = new DataConfig(config1.caseID);
	/* set combined data configuration */
	minX = (config1.getMinX() < config2.getMinX())?config1.getMinX():config2.getMinX();
	minY = (config1.getMinY() < config2.getMinY())?config1.getMinY():config2.getMinY();
	maxX = (config1.getMaxX() > config2.getMaxX())?config1.getMaxX():config2.getMaxX();
	maxY = (config1.getMaxY() > config2.getMaxY())?config1.getMaxY():config2.getMaxY();
	combinedConfig.setSpaceDimensions(minX, minY, maxX, maxY);
	combinedConfig.setSpaceObjects(config1.getSpaceObjects() + config2.getSpaceObjects());

	//combinedConfig.setPartitionBucketSize(partitionSize);
	this.partitionSize = partitionSize;
    }
    
    /**
     * Data formats:
     * 1: config.mappedPartitions: <loadtile-id> <polygon-id> <polygon>
     * 2: data (after reformat): <setNumber> <loadtile-id> <polygon-id> <polygon>
     * 3: joinMapData (after JNI): 
     *       <combinedtile-id> <join-idx> <setNumber> <loadtile-id> <polygon-id> <polygon>
     */
    public JavaRDD<TileStats> execute(){

	/* 
	 * POSSIBLE IMPROVEMENT: Use corgroup instead of groupByKey() 
	 */
	JavaPairRDD<Integer, Iterable<String>> groupedMapData = getDataGroupedByTile();
	
	/* Native C++: Resque */
	if (hmType == HMType.TILEDICE){
	    JavaPairRDD<Integer, Double> tileDiceResults = 
		groupedMapData.mapValues(new ResqueTileDice(
							    predicate.value,
							    config1.getGeomid(),
							    config2.getGeomid()
							    )
					 ).filter(new Function<Tuple2<Integer, Double>, Boolean>(){
						 public Boolean call(Tuple2<Integer, Double> t){
						     if (t._2() == -1)
							 return false;
						     return true;
						 }
					     });
	    return Coefficient.mapResultsToTile(
						this.partitionIDX, 
						tileDiceResults,
						hmType
						);
	}
	else{
	    JavaPairRDD<Integer, Iterable<String>> results = 
		groupedMapData.mapValues(new Resque(
    					      predicate.value, 
    					      config1.getGeomid(),
    					      config2.getGeomid())
				     );	
    	JavaRDD<Iterable<String>> vals = results.values();

    	/* Call Jaccard function to calculate jaccard coefficients per tile */
    	return Coefficient.execute(
    				   results.values(),
    				   /*spJoinResult,*/ 
    				   this.partitionIDX,
    				   hmType
    				   );
	}	
    }
    
    public JavaPairRDD<Integer, Iterable<String>> getDataGroupedByTile(){
	JavaRDD<String> data1 = config1.originalData.map(new Reformat(1));
    	JavaRDD<String> data2 = config2.originalData.map(new Reformat(2));

    	/* Join both datasets in same RDD */
    	JavaRDD<String> combinedData = data1.union(data2);
	
	// build index file
    	/******* Stage-2: Fixed-Grid Partitioning ******/	
    	// combinedConfig.setPartitionIDX(
    	// 			       Partitioner.fixedGridHM(
    	// 						       combinedConfig.getMinX(), 
    	// 						       combinedConfig.getMinY(), 
    	// 						       combinedConfig.getMaxX(), 
    	// 						       combinedConfig.getMaxY(), 
    	// 						       combinedConfig.getPartitionBucketSize()
    	// 						       )
    	// 			       );
	partitionIDX = Partitioner.fixedGrid(
					     combinedConfig.getSpanX(), 
					     combinedConfig.getSpanY(), 
					     this.partitionSize,
					     combinedConfig.getSpaceObjects()
					     );
	denormalizePartitionIDX(
				partitionIDX,
				combinedConfig.getMinX(),
				combinedConfig.getMinY(),
				combinedConfig.getSpanX(), 
				combinedConfig.getSpanY()
				);
	//combinedConfig.setPartitionIDX(pIDX);
	
	// IMPORTANT: DENORMALIZE COMBINED PARTITION FILE (MISSING)

	/* 
	 * POSSIBLE IMPROVEMENT: Broadcast ssidx 
	 * ssidx is not very big, will this help???
	 */
    	final SparkSpatialIndex ssidx = new SparkSpatialIndex();
    	ssidx.build(partitionIDX);
    	JavaPairRDD<Integer, String> joinMapData = 
    	    combinedData.flatMapToPair(new PartitionMapperJoin(ssidx, config1.getGeomid()));

    	JavaPairRDD<Integer, Iterable<String>> groupedMapData = joinMapData.groupByKey();
	return groupedMapData;
    }

    /**
     * Called for all data corresponding to a given key after groupByKey()
     */
    class Resque implements Function<Iterable<String>, Iterable<String>>{
    	private final String predicate;
    	private final int geomid1;
    	private final int geomid2;
    	public Resque(String predicate, int geomid1, int geomid2){
    	    this.predicate = predicate;
    	    this.geomid1 = geomid1;
    	    this.geomid2 = geomid2;
    	}
    	public Iterable<String> call (final Iterable<String> inData){
    	    List<String> ret = new ArrayList<String>();
    	    ArrayList<String> data = new ArrayList<String>();
    	    for (String in : inData)
    		data.add(in);	    
	    
    	    String[] dataArray = new String[data.size()];	    
    	    JNIWrapper jni = new JNIWrapper();
    	    String[] results = jni.resque(
    					  data.toArray(dataArray),
    					  predicate,
    					  geomid1,
    					  geomid2
    					  );
    	    for (String res : results)
    		ret.add(res);
    	    return ret;
    	}
    }
    
    /**
     * Replicated code. Fixed in newer version
     */
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
     * @return tileID,joinIDX,setNumber,id,polygon
     */
    class PartitionMapperJoin implements PairFlatMapFunction<String, Integer, String>{
    	private final SparkSpatialIndex ssidx;
    	private final int geomID;
    	public PartitionMapperJoin(SparkSpatialIndex ssidx, int geomID){
    	    this.ssidx = ssidx;
    	    this.geomID = geomID;
    	}
    	public Iterator<Tuple2<Integer, String>> call (final String line){
    	    String[] fields = line.split(String.valueOf(SparkGIS.TAB));

    	    int joinIDX = (fields[0].equals("1"))? 2 : 1;
    	    List<Tuple2<Integer, String>> ret = new ArrayList<Tuple2<Integer, String>>();
	    
    	    List<Long> tileIDs = ssidx.getIntersectingIndexTiles(fields[geomID]);
    	    for (long id : tileIDs){
    		String retLine = id + "\t" + joinIDX + "\t" + line;
    		Tuple2<Integer, String> t = new Tuple2<Integer, String>((int)id, retLine);
    		ret.add(t);
    	    }
    	    return ret.iterator();
    	}
    }

    /**
     * @return setNumber,id,polygon
     */
    class Reformat implements Function<Polygon, String>{
    	private final int setNumber;
    	public Reformat(int setNumber){this.setNumber = setNumber;}
    	public String call(final Polygon p){
	    return "" + this.setNumber + SparkGIS.TAB + p.toString();
    	}
    }

    /**
     * PARTFILE DENORMALIZATION
     * ADD TO NEWER VERSION
     * @param gMinX Global minimum x
     * @param gMinY Global minimum y
     * @param gSpanX Global span x
     * @param gSpanY Global span y
     * @return denormalized partition index
     */
    private void denormalizePartitionIDX(
					 List<Tile> partitionIDX, 
					 double gMinX,
					 double gMinY,
					 double gSpanX,
					 double gSpanY
					 ){
	for (Tile t:partitionIDX){
	    t.minX = t.minX * gSpanX + gMinX;
	    t.maxX = t.maxX * gSpanX + gMinX;
	    t.minY = t.minY * gSpanY + gMinY;
	    t.maxY = t.maxY * gSpanY + gMinY;
	}
    }
}
