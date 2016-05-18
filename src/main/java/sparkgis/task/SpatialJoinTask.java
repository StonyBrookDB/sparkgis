package sparkgis.task;
// /* Java imports */
import java.util.List;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.core.data.Polygon;
import sparkgis.core.data.SPJResult;
import sparkgis.core.data.TileStats;
import sparkgis.core.io.ISparkGISIO;
import sparkgis.core.data.DataConfig;
import sparkgis.core.enums.Predicate;
import sparkgis.core.enums.Delimiter;
import sparkgis.core.executionlayer.SparkPrepareData;
import sparkgis.core.executionlayer.SparkSpatialJoin;

public class SpatialJoinTask extends Task implements Callable<String>{
    //private final List<String> algos;
    private final Predicate predicate;
    
    private final String dataPath1;
    private final String dataPath2;
    private final Delimiter delim;
    private String outPath;
    
    
    public SpatialJoinTask(ISparkGISIO inputSrc, String dataPath1, String dataPath2, Delimiter delim, Predicate predicate, ISparkGISIO outDest){
	super(inputSrc, outDest, 2);
	this.dataPath1 = dataPath1;
	this.dataPath2 = dataPath2;
	this.delim = delim;
	this.predicate = predicate;
    }
    
    // output path for Hadoop supported Output 
    public void setOutPath(String outPath){this.outPath = outPath;}
    public void setPartitionSize(int pSize){super.pSize = pSize;}
    
    
    /**
     * Each HeatMapTask consists of 2 steps
     *   1. Generate configurations for algorithm pairs of input data (parallel)
     *   2. Generate heatmap from configurations
     */
    @Override
    public String call(){
	
	DataConfig config1 = new DataConfig(dataPath1);
	DataConfig config2 = new DataConfig(dataPath2);
	
	SparkGIS.Debug("Start Data Preparation ...");
	LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
	params.put("path", dataPath1);
	Future<DataConfig> f1 = super.exeService.submit(new AsyncPrepareData(params));
	// Can be improved ...
	LinkedHashMap<String, Object> params2 = new LinkedHashMap<String, Object>();
	params.put("path", dataPath2);
	Future<DataConfig> f2 = super.exeService.submit(new AsyncPrepareData(params2));

	try{
	    config1 = f1.get();
	    config2 = f2.get();
	}catch(Exception e){e.printStackTrace();}
	// close thread pool
	exeService.shutdown();

	/* Step-2: Generate heatmap from configurations */
	if ((config1 != null) && (config2 != null)){
	    // calculate SpatialJoin
	    SparkSpatialJoin spj = new SparkSpatialJoin(config1, config2, predicate, super.pSize);
	    //JavaRDD<SPJResult> result = spj.execute();
	    JavaPairRDD<Integer, Iterable<String>> result = spj.execute();
	    //outDest.writeRDD(result, outPath);
	    
	    //SparkGIS.Debug("Result Count: " + result.count());
	    
	    //sparkgis.stats.Profile.printProgress();
	}
	else System.out.println("Unexpected data configurations for: " + dataPath1 + "OR " + dataPath2);
	return "";
    }
}
