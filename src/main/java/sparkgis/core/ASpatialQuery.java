package sparkgis.core;
/* Java imports */
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.Serializable;
/* JTS imports */
import com.vividsolutions.jts.io.WKBWriter;
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
import sparkgis.coordinator.SparkGISContext;
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
public abstract class ASpatialQuery<T, R> implements Serializable{

    protected final SparkGISJobConf sgjConf;
    protected final Predicate predicate;
    protected final DataConfig<T> config1;
    protected final DataConfig<T> config2;
    protected final Space combinedSpace;


    /*
     * Combined configuration values removed from DataConfig
     * No need to inflate DataConfig object
     * since it has to be transferred over to workers
     */
    protected Broadcast<SparkSpatialIndex> ssidxBV = null;
    protected final List<Tile> partitionIDX;

    protected final SparkSpatialIndex ssidx;

    public ASpatialQuery(
			SparkGISJobConf sgjConf,
			DataConfig<T> config1,
			DataConfig<T> config2,
			Predicate predicate
			){
	this(sgjConf, config1, config2, predicate, null);
    }

    public ASpatialQuery(
			SparkGISJobConf sgjConf,
			DataConfig<T> config1,
			DataConfig<T> config2,
			Predicate predicate,
			List<Tile> partitionIDX
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
	if (partitionIDX == null)
	    this.partitionIDX = generateTiles();
	else{
	    /* no need to recompute partitionIDX/Tiles */
	    this.partitionIDX = partitionIDX;
	}

	/*
	 * Create and Broadcast spatial index on tile boundaries
	 */
	ssidx = new SparkSpatialIndex();
    	ssidx.build(this.partitionIDX);
	ssidxBV = SparkGISContext.sparkContext.broadcast(ssidx);
    }

    /**
     * A generic SpatialQuery execute method to be
     * implemented by concrete Query classes
     */
    public abstract JavaRDD<R> execute();

    private List<Tile> generateTiles(){
	List<Tile> ret;
	if (this.sgjConf.getPartitionMethod() == PartitionMethod.FIXED_GRID){
	    ret = Partitioner.fixedGrid(
					combinedSpace.getSpanX(),
					combinedSpace.getSpanY(),
					this.sgjConf.getPartitionSize(),
					combinedSpace.getSpaceObjects()
					);
	    denormalizePartitionIDX(
				    ret,
				    combinedSpace.getMinX(),
				    combinedSpace.getMinY(),
				    combinedSpace.getSpanX(),
				    combinedSpace.getSpanY()
				    );
	}
	else if (this.sgjConf.getPartitionMethod() == PartitionMethod.FIXED_GRID_HM){
	    ret = Partitioner.fixedGridHM(
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
	return ret;
    }

    /*
     * Cogroup version
     */
    protected JavaPairRDD<Integer, Tuple2<Iterable<T>, Iterable<T>>> getDataByTile(){

	/*
	 * Reformat stage only appends a set number to data from algo1 and algo2
	 * It has been merged with Partition mapper join stage
	 */

    	JavaPairRDD<Integer, T> joinMapData1 =
    	    config1.getData().flatMapToPair(new PartitionMapperJoin());

	JavaPairRDD<Integer, T> joinMapData2 =
    	    config2.getData().flatMapToPair(new PartitionMapperJoin());

	JavaPairRDD<Integer, Tuple2<Iterable<T>, Iterable<T>>> groupedData =
	    joinMapData1.cogroup(joinMapData2);

	return groupedData;
    }
    
    /**
     * Called for all data corresponding to a given key after groupByKey()
     * Return String format: Polygon1 Polygon2 Area1 Area2 Jaccard Dice
     */
    protected class Resque
	implements Function<Tuple2<Iterable<T>,Iterable<T>>, Iterable<String>>{

	private final int predicate;
    	private final int geomid1;
    	private final int geomid2;

    	public Resque(int predicate, int geomid1, int geomid2){
    	    this.predicate = predicate;
    	    this.geomid1 = geomid1;
    	    this.geomid2 = geomid2;
	}

    	public Iterable<String> call (final Tuple2<Iterable<T>,Iterable<T>> inData){
    	    //List<String> ret = new ArrayList<String>();
	    
    	    // ArrayList<String> data = new ArrayList<String>();
	    // short joinIDX = 2;
	    // for (T in : inData._1()){
	    // 	if (in instanceof SpatialObject){
	    // 	    data.add(((SpatialObject)in).getTileID() +
	    // 		     "\t" +
	    // 		     joinIDX +
	    // 		     "\t" +
	    // 		     ((SpatialObject)in).getSpatialData());
	    // 	}
	    // 	else
	    // 	    throw new RuntimeException("[ASpatialQuery-Resque] Not implemented yet");
	    // }
	    
	    // joinIDX = 1;
	    // for (T in : inData._2()){
	    // 	if (in instanceof SpatialObject){
	    // 	    data.add(((SpatialObject)in).getTileID() +
	    // 		     "\t" +
	    // 		     joinIDX +
	    // 		     "\t" +
	    // 		     ((SpatialObject)in).getSpatialData());
	    // 	}
	    // 	else
	    // 	    throw new RuntimeException("[ASpatialQuery-Resque] Not implemented yet");
	    // }

    	    // String[] dataArray = new String[data.size()];
    	    // JNIWrapper jni = new JNIWrapper();
    	    // String[] results = jni.resqueSPJ(
    	    // 				  data.toArray(dataArray),
    	    // 				  predicate,
    	    // 				  geomid1,
    	    // 				  geomid2
    	    // 				  );

	    String[] results;
	    JNIWrapper jni = new JNIWrapper();
	    // results = jni.resqueSPJIter(inData._1().iterator(), inData._2().iterator(), predicate, false);

	    T element = inData._1().iterator().next();
	    ArrayList<String> data1 = new ArrayList<String>();
	    ArrayList<String> data2 = new ArrayList<String>();
	    
	    if (element instanceof SpatialObject){
	    	
	    	for (T in : inData._1()){
	    	    data1.add(((SpatialObject)in).getSpatialData());
	    	}
	    	for (T in : inData._2()){
	    	    data2.add(((SpatialObject)in).getSpatialData());
	    	}
	    	results = jni.resqueSPJIter(data1.iterator(), data2.iterator(), predicate, false);
	    }
	    else if (element instanceof byte[]){
		for (T in : inData._1()){
	    	    data1.add(WKBWriter.toHex((byte[])in));
	    	}
	    	for (T in : inData._2()){
	    	    data2.add(WKBWriter.toHex((byte[])in));
	    	}
		results = jni.resqueSPJIter(inData._1().iterator(), inData._2().iterator(), predicate, true);
	    }
	    else
	    	throw new RuntimeException("[ASpatialQuery-Resque] Invalid input data type");

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
    private class PartitionMapperJoin implements PairFlatMapFunction<T, Integer, T>{

    	public Iterator<Tuple2<Integer, T>> call (final T s){
	    boolean assignedOnce = false;
    	    /* get spatial index from braodcast variable */
    	    final SparkSpatialIndex ssidx = ssidxBV.value();
    	    // final int joinIDX = (setNumber==1)? 2 : 1;

    	    List<Tuple2<Integer, T>> ret = new ArrayList<Tuple2<Integer, T>>();
	    List<Long> tileIDs;
	    if (s instanceof SpatialObject){
		tileIDs = ssidx.getIntersectingIndexTiles((SpatialObject)s);

		for (long id : tileIDs){
		    //String retLine = id + "\t" + joinIDX + "\t" + this.setNumber + "\t" + s.toString();
		    // Tuple2<Integer, T> t = new Tuple2<Integer, R>((int)id, retLine);
		    if (assignedOnce){
			/*
			 * make a copy of spatial object
			 * should only happen for cross boundary objects
			 */
			SpatialObject nSO = new SpatialObject(((SpatialObject)s).getId(),
							      ((SpatialObject)s).getSpatialData());
			nSO.setTileID(id);
			Tuple2<Integer, T> t = new Tuple2<Integer, T>((int)id, (T)nSO);
			ret.add(t);
		    }
		    else{
			((SpatialObject)s).setTileID(id);
			Tuple2<Integer, T> t = new Tuple2<Integer, T>((int)id, s);
			ret.add(t);
			assignedOnce = true;
		    }
		}
	    }
	    else if (s instanceof byte[]){
		tileIDs = ssidx.getIntersectingIndexTiles((byte[])s);
		for (long id : tileIDs){
		    Tuple2<Integer, T> t = new Tuple2<Integer, T>((int)id, s);
		    ret.add(t);
		}
	    }
	    else
		throw new RuntimeException("[ASpatialQuery-PartitionMapperJoin] Invalid input data type");
	    
    	    
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
