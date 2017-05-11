package sparkgis.coordinator.functions;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.core.task.SpatialJoinTask;

public class SpatialRange{

    
    /**
     * Spatial Range function. Reads in raw data, preprocess it and apply spatial range 
     * @param spgc SparkGISContext 
     * @param datasets List of String or DataConfigs to generate heatmap for in parallel
     * @param pred Predicate to use for spatial join
     *                               (required only if writing result to MongoDB)  
     * @return String RDD. Each String contains information of spatial objects
     */
    public static JavaRDD<Iterable<String>> execute(SparkGISContext spgc,
					  List datasets,
					  Predicate pred){

	if (datasets.get(0) instanceof String){
	    /* Perform spatial join from start */
	    SpatialJoinTask t = new SpatialJoinTask(spgc, datasets, pred);
	    return t.call();
	}
	else if (datasets.get(0) instanceof DataConfig){
	    /* Already preprocessed data, go directly to performing spatial join */
	    SpatialJoinTask t = new SpatialJoinTask(spgc, pred);
	    return t.call(datasets);
	}
	else{
	    throw new java.lang.UnsupportedOperationException("Should not reach here");
	}
    }

    /**
     * 
     */
    public static JavaRDD<String> execute(){
	throw new java.lang.UnsupportedOperationException("SpatialRange with RDDs not implemented yet");
    }
}
