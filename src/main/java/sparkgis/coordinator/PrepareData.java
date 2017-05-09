package sparkgis.coordinator;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Spark imports */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
/* Local imports */
import sparkgis.data.DataConfig;
import sparkgis.data.SpatialObject;
import sparkgis.data.SpatialObjectDataConfig;

public class PrepareData implements Serializable{

    private final int index;
    private final String delimiter;
    
    public PrepareData(String delimiter, int index){
	this.delimiter = delimiter;
	this.index = index;
    }
    
    /**
     * Read spatial data into w.k.t format RDD from HDFS, local file system (available
     * on all nodes), or any Hadoop supported file system URI
     * @param dataPath Data URI on HDFS, local file system or any Hadoop supported file system
     * @param withID If true, get spatial data ID from input source (geometryIndex-1)
     * @return RDD of spatial data in w.k.t format 
     */
    public JavaRDD<SpatialObject> getTextAsSpatialString(String dataPath, final boolean withID){
	return SparkGISContext.sparkContext.textFile(dataPath, SparkGISContext.sparkContext.defaultParallelism())
	    .filter(new Function<String, Boolean>(){
		    public Boolean call(String s) {return (!s.isEmpty());}
		})
	    .map(new Function<String, SpatialObject>(){
		    public SpatialObject call(String s){
			String[] fields = s.split(delimiter);
			int spdIndex = index;
			if (withID)
			    return new SpatialObject(fields[spdIndex-1], fields[spdIndex]);
			
			return new SpatialObject(fields[spdIndex]);
		    }
		});
    }
    
    /**
     * A spatial data query usually consists of atleast two datasets
     * e.g. spatial join, kNN, Range etc. 
     * Before actual query processing, spatial data needs to be preprocessed
     * e.g. extract MBBs, create partitions, create indices etc. This 
     * preprocessing can usually be done independently.
     * This function allows multiple spatial datasets to be preprocessed
     * concurrently. 
     */
    public List<DataConfig> prepareData(List<String> dataPaths){
	final int datasetCount = dataPaths.size();
	List<DataConfig> configs = new ArrayList<DataConfig>(datasetCount);
	List<Future<DataConfig>> futures = new ArrayList<Future<DataConfig>>(datasetCount);
	/* To generate data configurations in parallel */
	final ExecutorService exeServ = Executors.newFixedThreadPool(datasetCount);
	
	for (int i=0; i<datasetCount; ++i){
	    futures.add(exeServ.submit(new AsyncPrepareData(dataPaths.get(i))));
	}
	/* wait for all configurations to get complete */
	try{
	    for (int i=0; i<datasetCount; ++i)
		configs.add(futures.get(i).get());
	}catch(Exception e){e.printStackTrace();}
	/* close thread pool */
	exeServ.shutdown();
	return configs;
    }

    /**
     * Inner class to get data and generate data configuration
     */
    private class AsyncPrepareData implements Callable<DataConfig>{
        private final String dataPath;	
    	public AsyncPrepareData(String dataPath){
    	    this.dataPath = dataPath;
    	}
	
    	@Override
    	public DataConfig call(){
    	    /* get data from input source and keep in memory */
	    JavaRDD<SpatialObject> spatialDataRDD =
		getTextAsSpatialString(dataPath, true).cache();
	    long objCount = spatialDataRDD.count();
    	    if (objCount != 0){
		/* Invoke spark job: Prepare Data */
    		DataConfig ret = new SpatialObjectDataConfig(dataPath, spatialDataRDD);
		ret.prepare();
    		return ret;
    	    }
    	    return null;
    	}
    }
}
