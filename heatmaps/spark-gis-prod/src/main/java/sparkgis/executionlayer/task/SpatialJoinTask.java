package sparkgis.executionlayer.task;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Callable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.executionlayer.SparkSpatialJoin;

public class SpatialJoinTask extends Task implements Callable<JavaRDD<String>>{

    private final List<String> datasetPaths;
    private final Predicate predicate;

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
    public JavaRDD<String> call(){

	SparkSpatialJoin spj = null;
	List<DataConfig> configs = sgc.prepareData(this.datasetPaths);

	/* generate pairs of all datasets */
	final List<Integer> pairs = generatePairs(this.datasetPaths.size());
	List<JavaRDD<String>> results = new ArrayList<JavaRDD<String>>(pairs.size());
	
	for (int i=0; i<pairs.size(); i+=2){
	    /* perform spatial join from configuration pairs */
	    if ((configs.get(i) != null) && (configs.get(i+1) != null)){
		/* generate heatmap based from data configurations */
		spj = new SparkSpatialJoin(sgc, configs.get(i), configs.get(i+1), predicate);
		results.add(spj.execute());
	    }
	    else
		System.out.println("Unexpected data configurations for dat:"+super.data);
	}

	/* Take union of all rdds and */
	if (results.size() > 1){
	    return sgc.union(results.get(0), results.subList(1, results.size()));
	}
	return results.get(0);
    }
}
