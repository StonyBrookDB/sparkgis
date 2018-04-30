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


public abstract class ASpatialQuery<T> implements Serializable{

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
	public  ASpatialQuery(
			SparkGISJobConf sgjConf,
			DataConfig<SpatialObject> config1,
			DataConfig<SpatialObject> config2,
			Predicate predicate){
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
