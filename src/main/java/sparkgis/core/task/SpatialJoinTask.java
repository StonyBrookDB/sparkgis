package sparkgis.core.task;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Callable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
/* Local imports */
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.core.SparkSpatialJoin;
import sparkgis.coordinator.SparkGISContext;

public class SpatialJoinTask extends Task implements Callable<JavaRDD<Iterable<String>>>{

    private final List<String> datasetPaths;
    private final Predicate predicate;

    public SpatialJoinTask(SparkGISContext sgc,
			   Predicate predicate){
	super(sgc, "");
	this.datasetPaths = null;
	this.predicate = predicate;
    }
    
    public SpatialJoinTask(SparkGISContext sgc,
			   List<String> datasetPaths,
			   Predicate predicate){
	super(sgc, datasetPaths.get(0));
	
	this.datasetPaths = datasetPaths;
	this.predicate = predicate;
    }
    
    /**
     * Each SpatialJoinTask consists of 2 steps
     *   1. Generate configurations for all datasets (parallel)
     *   2. Perform pairwise spatial join on all datasets
     */
    @Override
    public JavaRDD<Iterable<String>> call(){
	List<DataConfig> configs = sgc.prepareData(this.datasetPaths);

	return call(configs);
    }

    public JavaRDD<Iterable<String>> call(List<DataConfig> configs){
	SparkSpatialJoin spj = null;
	/* generate pairs of all datasets */
	final List<Integer> pairs = super.generatePairs(configs.size());
	List<JavaRDD<Iterable<String>>> results = new ArrayList<JavaRDD<Iterable<String>>>(pairs.size());
	
	for (int i=0; i<pairs.size(); i+=2){
	    /* perform spatial join from configuration pairs */
	    if ((configs.get(i) != null) && (configs.get(i+1) != null)){
		spj = new SparkSpatialJoin(sgc.getJobConf(), configs.get(i), configs.get(i+1), predicate);
		results.add(spj.execute());
	    }
	    else
		System.out.println("Unexpected data configurations for dat:"+super.data);
	}

	/* Take union of all rdds and */
	if (results.size() > 1){
	    return SparkGISContext.sparkContext.union(results.get(0), results.subList(1, results.size()));
	}
	return results.get(0);
    }
}
