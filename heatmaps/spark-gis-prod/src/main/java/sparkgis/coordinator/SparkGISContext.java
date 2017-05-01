package sparkgis.coordinator;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Spark imports */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
/* JTS imports */
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.ByteOrderValues;
/* Local imports */
import sparkgis.data.DataConfig;
import sparkgis.data.BinaryDataConfig;
import sparkgis.data.SpatialObject;
import sparkgis.core.SparkGISPrepareData;
import sparkgis.core.SparkGISPrepareBinaryData;

public class SparkGISContext extends JavaSparkContext{

    private final SparkGISJobConf jobConf;
    
    /**
     * SparkGISContext constructor with default SparkGISJobConf
     */
    public SparkGISContext(){
	super();
	this.jobConf = new SparkGISJobConf();
    }

    /**
     * SparkGISContext constructor with cutom configurations
     * @param conf SparkConf object
     * @param jobConf Custom SparkGIS job configuration
     */
    public SparkGISContext(SparkConf conf, SparkGISJobConf jobConf){
	super(conf);
	this.jobConf = jobConf;
    }

    /**
     * @return Current SparkGIS job configuration
     */
    public SparkGISJobConf getJobConf(){return this.jobConf;}
    
    /**
     * Read spatial data into w.k.t format RDD from HDFS, local file system (available
     * on all nodes), or any Hadoop supported file system URI
     * @param dataPath Data URI on HDFS, local file system or any Hadoop supported file system
     * @param withID If true, get spatial data ID from input source (geometryIndex-1)
     * @return RDD of spatial data in w.k.t format 
     */
    public JavaRDD<SpatialObject> getTextAsSpatialString(String dataPath, final boolean withID){
	return this.textFile(dataPath, this.defaultParallelism())
	    .filter(new Function<String, Boolean>(){
		    public Boolean call(String s) {return (!s.isEmpty());}
		})
	    .map(new Function<String, SpatialObject>(){
		    public SpatialObject call(String s){
			String[] fields = s.split(jobConf.getDelimiter());
			int spdIndex = jobConf.getSpatialObjectIndex();
			if (withID)
			    return new SpatialObject(fields[spdIndex-1], fields[spdIndex]);
			
			return new SpatialObject(fields[spdIndex]);
		    }
		});
    }

    /**
     * Read spatial data into w.k.b format RDD from HDFS, local file system (available
     * on all nodes), or any Hadoop supported file system URI
     * @param dataPath Data URI on HDFS, local file system or any Hadoop supported file system
     * @param withID If true, get spatial data ID from input source (geometryIndex-1)
     * @return RDD of spatial data in w.k.t format 
     */
    public JavaRDD<byte[]> getTextAsByteArray(String dataPath, final boolean withID){
	return this.textFile(dataPath, this.defaultParallelism())
	    .filter(new Function<String, Boolean>(){
		    public Boolean call(String s) {return (!s.isEmpty());}
		})
	    .map(new Function<String, byte[]>(){
		    public byte[] call(String s){
			String[] fields = s.split(jobConf.getDelimiter());
			int spdIndex = jobConf.getSpatialObjectIndex();
			// if (withID)
			//     return new SpatialObject(fields[spdIndex-1], fields[spdIndex]);
			
			// return new SpatialObject(fields[spdIndex]);
			try{
			    WKBWriter w = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
			    WKTReader reader = new WKTReader();
			    return w.write(reader.read(fields[spdIndex]));
			}catch(Exception e){e.printStackTrace();}
			return null;
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
    		SparkGISPrepareData job = new SparkGISPrepareData(dataPath);
    		DataConfig ret = job.execute(spatialDataRDD);
    		return ret;
    	    }
    	    return null;
    	}
    }

    /**
     * Inner class to get binary data and generate data configuration
     */
    private class AsyncPrepareBinaryData implements Callable<BinaryDataConfig>{
        private final String dataPath;	
    	public AsyncPrepareBinaryData(String dataPath){
    	    this.dataPath = dataPath;
    	}
	
    	@Override
    	public BinaryDataConfig call(){
    	    /* get data from input source and keep in memory */
	    JavaRDD<byte[]> spatialDataRDD =
		getTextAsByteArray(dataPath, true).cache();
	    long objCount = spatialDataRDD.count();
    	    if (objCount != 0){
    		/* Invoke spark job: Prepare Data */
    		SparkGISPrepareBinaryData job = new SparkGISPrepareBinaryData(dataPath);
    		BinaryDataConfig ret = job.execute(spatialDataRDD);
    		return ret;
    	    }
    	    return null;
    	}
    }
}
