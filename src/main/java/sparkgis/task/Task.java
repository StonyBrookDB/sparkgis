package sparkgis.task;

/* Java imports */
import java.util.Map;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
/* Local imports*/
import sparkgis.SparkGIS;
import sparkgis.core.data.Polygon;
import sparkgis.core.data.DataConfig;
import sparkgis.core.io.ISparkGISIO;
import sparkgis.core.executionlayer.SparkPrepareData;

public class Task{
    protected final ISparkGISIO inputSrc;
    protected final ISparkGISIO outDest;
    //protected final String dataID;

    protected int pSize = 512; // Default partition size

    // To generate data configurations in parallel
    protected final int batchFactor;
    protected final ExecutorService exeService;

    public Task(ISparkGISIO inputSrc, ISparkGISIO outDest, int batchFactor){
	this.inputSrc = inputSrc;
	this.outDest = outDest;
	//this.dataID = dataID;
	this.batchFactor = batchFactor;
	exeService = Executors.newFixedThreadPool(batchFactor);
    }
    
    /**
     * Every task has to readin data from input source and prepare it for spatial processing
     */
    protected class AsyncPrepareData implements Callable<DataConfig>{
        //private final String dataPath;
	private final Map<String, Object> params;
    	//public AsyncPrepareData(String dataPath){
	//    this.dataPath = dataPath;
    	//}
	public AsyncPrepareData(Map<String, Object> params){
	    this.params = params;
    	}
	
	@Override
	public DataConfig call(){
	    long start = System.nanoTime();
	    //SparkGIS.Debug("Start reading data: " + dataPath);
	    // get data from input source and keep in memory
	    JavaRDD<Polygon> polygonsRDD = 
		inputSrc.getPolygonsRDD(params).persist(StorageLevel.MEMORY_AND_DISK_SER());
		// inputSrc.getPolygonsRDD(dataPath, delim).persist(StorageLevel.MEMORY_AND_DISK_SER());
	    String caseID = (String)params.get("caseID");
	    long objCount = (long)polygonsRDD.count();	
	    SparkGIS.Debug("Done reading data. Starting data preparation ...");
	    start = System.nanoTime();
	    if (objCount != 0){
		// Invoke spark job: Prepare Data
		SparkPrepareData job = new SparkPrepareData(caseID);
		DataConfig ret = job.execute(polygonsRDD);
		return ret;
	    }
	    return null;
	}
    }
}
