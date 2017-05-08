package sparkgis.pia;
/* Java imports */
// import java.util.List;
// import java.util.Arrays;
// import java.util.Iterator;
// import java.util.ArrayList;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
// import org.apache.spark.broadcast.Broadcast;
// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.function.Function;
// import org.apache.spark.api.java.function.FlatMapFunction;
// import org.apache.spark.api.java.function.PairFunction;
// import org.apache.spark.api.java.function.PairFlatMapFunction;
// import org.apache.spark.api.java.function.Function2;
// import scala.Tuple2;
/* Local imports */
import jni.JNIWrapper;
import sparkgis.data.Tile;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.enums.HMType;
import sparkgis.data.SpatialObject;
import sparkgis.core.SparkSpatialJoin;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.executionlayer.partitioning.Partitioner;
import sparkgis.executionlayer.spatialindex.SparkSpatialIndex;

/**
 * Spark Spatial Join for HeatMap Generation
 */
public class SparkSpatialJoinHM extends SparkSpatialJoin implements Serializable{

    private final HMType hmType;
    
    public SparkSpatialJoinHM(
			      SparkGISContext sgc,
			      DataConfig config1,
			      DataConfig config2,
			      Predicate predicate,
			      HMType hmType
			      ){

	super(sgc, config1, config2, predicate);	
	this.hmType = hmType;
    }
    
    /**
     * Generates per tile similarity coefficient value. Currently supports Jaccard and Dice
     * @return JavaRDD<TileStats> Average coefficient result per tile \t tile coordinates
     */
    public JavaRDD<TileStats> execute(){
	
    	JavaRDD<String> results = super.execute();

    	/* Call Jaccard function to calculate jaccard coefficients per tile */
    	return Coefficient.execute(
    				   results,
    				   /*spJoinResult,*/ 
    				   partitionIDX,
    				   hmType
    				   );
    }
}
