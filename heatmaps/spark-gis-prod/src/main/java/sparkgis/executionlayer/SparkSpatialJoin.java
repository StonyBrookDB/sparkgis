package sparkgis.executionlayer;
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
import sparkgis.coordinator.SparkGISContext;
import sparkgis.data.SpatialObject;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.executionlayer.partitioning.Partitioner;
import sparkgis.executionlayer.spatialindex.SparkSpatialIndex;

/**
 * Spark Spatial Join
 */
public class SparkSpatialJoin implements Serializable{

    private final SparkGISContext sgc;
    
    private final Predicate predicate;
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
    private Broadcast<SparkSpatialIndex> ssidxBV = null;
    
    public SparkSpatialJoin(
			    SparkGISContext sgc,
			    DataConfig config1,
			    DataConfig config2,
			    Predicate predicate
			    ){
	this.sgc = sgc;
	this.predicate = predicate;
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
    }
    
    /**
     * Data formats:
     * 1: config.mappedPartitions: <loadtile-id> <spatialObject-id> <spatialObject>
     * 2: data (after reformat): <setNumber> <loadtile-id> <spatialObject-id> <spatialObject>
     * 3: joinMapData (after JNI): 
     *  <combinedtile-id> <join-idx> <setNumber> <loadtile-id> <spatialObject-id> <spatialObject>
     */
    public JavaRDD<String> execute(){
	
	List<Tile> partitionIDX =
	    Partitioner.fixedGrid(
				  combinedConfig.getSpanX(), 
				  combinedConfig.getSpanY(), 
				  this.sgc.getJobConf().getPartitionSize(),
				  combinedConfig.getSpaceObjects()
				  );
	denormalizePartitionIDX(
				partitionIDX,
				combinedConfig.getMinX(),
				combinedConfig.getMinY(),
				combinedConfig.getSpanX(), 
				combinedConfig.getSpanY()
				);

	/* 
	 * Broadcast ssidx 
	 * ssidx is not very big, will this help???
	 */
    	final SparkSpatialIndex ssidx = new SparkSpatialIndex();
    	ssidx.build(partitionIDX);
	ssidxBV = sgc.broadcast(ssidx);

	JavaPairRDD<Integer, Tuple2<Iterable<String>,Iterable<String>>>
	    groupedMapData = getDataByTile();

	/* Native C++: Resque */
	JavaPairRDD<Integer, String> results = 
	    groupedMapData.flatMapValues(new Resque(
						    predicate.value, 
						    config1.getGeomid(),
						    config2.getGeomid())
					 );	
    	return results.values();
    }

    /*
     * Cogroup version
     */
    public JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> getDataByTile(){

	/* 
	 * Reformat stage only appends a set number to data from algo1 and algo2 
	 * It has been merged with Partition mapper join stage
	 */
	
    	JavaPairRDD<Integer, String> joinMapData1 = 
    	    config1.originalData.flatMapToPair(new PartitionMapperJoin(1));

	JavaPairRDD<Integer, String> joinMapData2 = 
    	    config2.originalData.flatMapToPair(new PartitionMapperJoin(2));

	JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> groupedData =
	    joinMapData1.cogroup(joinMapData2);

	return groupedData;
    }

    /**
     * Called for all data corresponding to a given key after groupByKey()
     */
    class Resque
	implements Function<Tuple2<Iterable<String>,Iterable<String>>, Iterable<String>>{

	private final String predicate;
    	private final int geomid1;
    	private final int geomid2;
    	public Resque(String predicate, int geomid1, int geomid2){
    	    this.predicate = predicate;
    	    this.geomid1 = geomid1;
    	    this.geomid2 = geomid2;
    	}
    	public Iterable<String> call (final Tuple2<Iterable<String>,Iterable<String>> inData){
    	    ArrayList<String> data = new ArrayList<String>();
    	    for (String in : inData._1())
    		data.add(in);
	    for (String in : inData._2())
    		data.add(in);
	    
    	    String[] dataArray = new String[data.size()];	    
    	    JNIWrapper jni = new JNIWrapper();
    	    String[] results = jni.resque(
    					  data.toArray(dataArray),
    					  predicate,
    					  geomid1,
    					  geomid2
    					  );
    	    return Arrays.asList(results);
    	}
    }
    
    /**
     * Maps each spatialObject to a tile from spatial index after appending a set number to the data.
     * NOTE: There is a difference between joinIDX and setNumber
     * In case of any issue, please refer to Journal Entry: Jan 24, 2017
     * @param setNumber Dataset this spatialObject belongs to
     * @return tileID,joinIDX,setNumber,id,spatialObject
     */
    class PartitionMapperJoin implements PairFlatMapFunction<SpatialObject, Integer, String>{
	private final int setNumber;

	public PartitionMapperJoin(int setNumber){
	    this.setNumber = setNumber;
    	}
    	public Iterator<Tuple2<Integer, String>> call (final SpatialObject s){

	    /* get spatial index from braodcast variable */
	    final SparkSpatialIndex ssidx = ssidxBV.value();
	    final int joinIDX = (setNumber==1)? 2 : 1;
	    
    	    List<Tuple2<Integer, String>> ret = new ArrayList<Tuple2<Integer, String>>();
	    List<Long> tileIDs = ssidx.getIntersectingIndexTiles(s.getSpatialData());
    	    for (long id : tileIDs){
		String retLine = id + "\t" + joinIDX + "\t" + this.setNumber + "\t" + s.toString();
    		Tuple2<Integer, String> t = new Tuple2<Integer, String>((int)id, retLine);
    		ret.add(t);
    	    }
    	    return ret.iterator();
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
