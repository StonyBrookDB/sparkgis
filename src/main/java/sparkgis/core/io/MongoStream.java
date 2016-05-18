/**
 * Algo:
 * DCMongoDBDataAccess.java has an overloaded version of 
 *      JavaRDD getDataRDD(Map<String, Object> params, Long batchStart, long batchSize)
 *      JavaRDD<Long> getSplits(Long batchStart,Long batchSize,int splitSize)
 * 
 * Spark Streaming can save/update state using 
 *      updateStateByKey()
 * Or instead, Spark's accumulator can be used for the same purpose??
 *
 * Using this, keep track of number of mongodb objects in one stream
 * For the next stream, start reading after the number of objects read in previous stream
 */
package datacube.io.streaming;
/* Java imports */
import java.util.Map;
/* Spark imports */
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
/* Local imports */
import sparkgis.SparkGIS;
import datacube.data.DCObject;
import datacube.io.DCMongoDBDataAccess;


public class MongoStream extends Receiver<String>{

    final int count;
    final DCMongoDBDataAccess mongoIn;
    final Map<String, Object> params;

    final Long st = new Long(0);
    
    public static final Accumulator<Integer> objectsRead = SparkGIS.sc.accumulator(0);
	//SparkGIS.sc.accumulator(st);

    public MongoStream(final int count, final DCMongoDBDataAccess mongoIn, final Map<String, Object> params){
	super(StorageLevel.MEMORY_AND_DISK_2());

	this.count = count;
	this.mongoIn = mongoIn;
	this.params = params;
	//objectsRead = SparkGIS.sc.accumulator(0);
    }

    /********* Interface methods ********/
    public void onStart(){
	new Thread(){
	    public void run(){
		receive();
	    }
	}.start();
    }
    
    public void onStop(){
	
    }

    private void receive(){
	try{

	    JavaRDD<DCObject> objRDD = mongoIn.getDataRDD(params).cache();

	    long count = objRDD.count();
	    
	    objectsRead.add((int)count); // FIX THIS ...
	    
	    System.out.println("Stream size: " + count);
	    System.out.println("Accum: " + objectsRead.value());
	    
	    
	    // for (int i=0; i<this.count; ++i){
	    // 	store("Test values");

	    // 	objectsRead.add(5);

	    // 	System.out.println(objectsRead.value());
		
	    // 	Thread.sleep(500);
	    // }
	}catch(Exception e){
	    restart("Some exception in custom receiver", e);
	}
	
    }
}
