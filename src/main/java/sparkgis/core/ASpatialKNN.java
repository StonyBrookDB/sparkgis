package sparkgis.core;
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
import sparkgis.data.Space;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.data.SpatialObject;
import sparkgis.enums.PartitionMethod;
import sparkgis.coordinator.SparkGISJobConf;
import sparkgis.core.partitioning.Partitioner;
import sparkgis.core.spatialindex.SparkSpatialIndex;

/**
 * Spark Spatial Join
 * Data formats:
 * 1: config.mappedPartitions: <loadtile-id> <spatialObject-id> <spatialObject>
 * 2: data (after reformat): <setNumber> <loadtile-id> <spatialObject-id> <spatialObject>
 * 3: joinMapData (after JNI): 
 *  <combinedtile-id> <join-idx> <setNumber> <loadtile-id> <spatialObject-id> <spatialObject>
 *
 * T: Input spatial data type (SpatialObject OR byte[])
 * R: Return type (Iterable<String> OR TileStats)
 */
public abstract class ASpatialKNN<T>  implements Serializable{

    protected final SparkGISJobConf sgjConf;
    protected final Predicate predicate;
    protected final DataConfig<SpatialObject> config1;
    protected final DataConfig<SpatialObject> config2;
    protected final Space combinedSpace;
    
    /* 
     * Combined configuration values removed from DataConfig 
     * No need to inflate DataConfig object 
     * since it has to be transferred over to workers
     */
    protected Broadcast<SparkSpatialIndex> ssidxBV = null;

    protected List<Tile> partitionIDX;
    
    public ASpatialKNN(
			SparkGISJobConf sgjConf,
			DataConfig<SpatialObject> config1,
			DataConfig<SpatialObject> config2,
			Predicate predicate
			){
	this.sgjConf = sgjConf;
	this.predicate = predicate;
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

	generateTiles();
    }

    /**
     * A generic SpatialJoin execute method
     */
    public abstract JavaRDD<T> execute();

    private void generateTiles(){
	if (this.sgjConf.getPartitionMethod() == PartitionMethod.FIXED_GRID){
	    partitionIDX = Partitioner.fixedGrid(
						 combinedSpace.getSpanX(), 
						 combinedSpace.getSpanY(), 
						 this.sgjConf.getPartitionSize(),
						 combinedSpace.getSpaceObjects()
						 );
	    denormalizePartitionIDX(
				    partitionIDX,
				    combinedSpace.getMinX(),
				    combinedSpace.getMinY(),
				    combinedSpace.getSpanX(), 
				    combinedSpace.getSpanY()
				    );
	}
	else if (this.sgjConf.getPartitionMethod() == PartitionMethod.FIXED_GRID_HM){
	    partitionIDX = Partitioner.fixedGridHM(
						   combinedSpace.getMinX(), 
						   combinedSpace.getMinY(), 
						   combinedSpace.getMaxX(),
						   combinedSpace.getMaxY(),
						   this.sgjConf.getPartitionSize()
						   );
	}
	else{
	    throw new java.lang.RuntimeException("Invalid paritioner method");
	}
    }

    /*
     * Cogroup version
     */
    protected JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> getDataByTile(){

	/* 
	 * Reformat stage only appends a set number to data from algo1 and algo2 
	 * It has been merged with Partition mapper join stage
	 */
	
    	JavaPairRDD<Integer, String> joinMapData1 = 
    	    config1.getData().flatMapToPair(new PartitionMapperJoin(1));

	JavaPairRDD<Integer, String> joinMapData2 = 
    	    config2.getData().flatMapToPair(new PartitionMapperJoin(2));

	JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> groupedData =
	    joinMapData1.cogroup(joinMapData2);

	return groupedData;
    }

    /**
     * Called for all data corresponding to a given key after groupByKey()
     */
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
    	    return Arrays.asList(results);
    	}
    }
    
    /**
     *
     */
    protected class ResqueTileDice implements Function<Iterable<String>, Double>{
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
     * @return tileID,joinIDX,setNumber,id,spatialObject
     */
    protected class PartitionMapperJoin implements PairFlatMapFunction<SpatialObject, Integer, String>{
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
    protected void denormalizePartitionIDX(
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

