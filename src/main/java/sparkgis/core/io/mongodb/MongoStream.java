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
//package datacube.io.streaming;
package sparkgis.core.io.mongodb;
/* Java imports */
//import java.util.ArrayList;
//import java.util.List;
import java.util.Map;
import java.lang.reflect.*;
import java.io.Serializable;
import java.lang.RuntimeException;
import java.util.concurrent.Callable;
/* MongoDB imports */
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
/* Spark imports */
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.spark.api.java.function.VoidFunction;
/* Local imports */
import sparkgis.SparkGIS;
import datacube.data.DCObject;
import sparkgis.core.data.Polygon;
import sparkgis.core.data.MongoPolygon;

import sparkgis.SparkGISConfig;

import org.bson.Document;

public class MongoStream extends Receiver<DCObject>
implements Serializable
{

// Copied from ReadMongoSplit
private final Map<String, Object> params;
//reflection
private final String dataClassName;

//public static final Accumulator<Integer> objectsRead = SparkGIS.sc.accumulator(0);
//SparkGIS.sc.accumulator(st);

public MongoStream(Map<String, Object> params,
                   String dataClassName
                   ){
        super(StorageLevel.MEMORY_AND_DISK_2());

        if (!params.containsKey("db") ||
            !params.containsKey("collection")
            )
                throw new RuntimeException("[SparkGIS] Invalid filter parameters");

        this.params = params;
        this.dataClassName = dataClassName;
}


public void onStart(){
        new Thread(){
                public void run(){
                        getDataRDD();
                }
        }.start();
}

public void onStop(){
}

/**
 * Almost similar implementation as MongoDBDataAccess
 */
//public JavaRDD getDataRDD(Map<String, Object> params, String dataClassName){
public void getDataRDD(){


        Mongo.validate(params);

        final String host = (String)params.get("host");
        final int port = Integer.parseInt((String)params.get("port"));

        long objectsCount = 0;
        try{

                System.out.println("getDataRDD_1");
                final MongoClient mongoClient = new MongoClient(host, port);
                objectsCount = Mongo.getObjectsCount(params, mongoClient);
        }catch(Exception e) {System.out.println(e.toString()); }

        // final int nSplits = SparkGIS.sc.defaultParallelism();
        // final int splitSize = (int)objectsCount/nSplits;

        // JavaRDD<Long> splits = Mongo.getSplits(objectsCount, splitSize);

        // Different from MongoDBDataAccess
        //splits.foreach(new ReadMongoSplitStream(params, splitSize, dataClassName));
        Long start = new Long(0);
        // FIX: cast to int ...

        System.out.println("getDataRDD_2");
        (new ReadMongoSplitStream(params, (int)objectsCount, dataClassName)).call(start);

        System.out.println("getDataRDD_3");
        //System.out.println("[MongoStream] Objects Read: " + objectsRead.value());
}




/**
 * Stream read does not have to return anything since
 * it implements a Receiver
 * Receiver simply stores the read value, which can later be converted to DStream
 * for further processing
 */
class ReadMongoSplitStream
implements Serializable, VoidFunction<Long>
{
private final String host;
private final int port;
private final Map<String, Object> params;
private final int maxSplitSize;

//for reflection
private final String dataClassName;

public ReadMongoSplitStream(Map<String, Object> params,
                            int maxSplitSize,
                            String dataClassName
                            ){
        if (!params.containsKey("db") ||
            !params.containsKey("collection")
            )
                throw new RuntimeException("[SparkGIS] Invalid filter parameters");
        this.host = (String)params.get("host");
        this.port = Integer.parseInt((String)params.get("port"));
        this.params = params;
        this.maxSplitSize = maxSplitSize;

        this.dataClassName = dataClassName;
}

/**
 * VoidFunction interface method
 */
public void call(Long start){

        System.out.println("getDataRDD_call_1");

        // Reflection to get data extraction method
        Method extractDataMethod = null;
        Object dataClass = null;
        try{
                Class<?> c = Class.forName(dataClassName);
                dataClass = c.newInstance();
                extractDataMethod = c.getDeclaredMethod("extractData", new Class[] {DBObject.class });
        }catch(Exception e) {e.printStackTrace(); }

        MongoClient mongoClient = null;
        DBCursor cursor = null;

        try{
                mongoClient = new MongoClient(host, port);
                // cursor = Mongo.getDataSplit(params, start, maxSplitSize, mongoClient);

                String dbName = SparkGISConfig.db;
                String col = SparkGISConfig.collection;
                DB db =  mongoClient.getDB(dbName);
                DBCollection coll = db.getCollection(col);




                // params.put ("features.Area",new Integer(851) );
                // params.put(  "features.Perimeter", new BasicDBObject("$gt", "120").append("$lte", "122")   );

                // BasicDBObject query =Mongo.prepareQuery(params);
                cursor = Mongo.getDataSplit(params, start, maxSplitSize, mongoClient);
                // Document condition1 =new Document (      "features.Perimeter", new BasicDBObject("$gt", 120).append("$lte", 122)   );
                // DBObject condition2 = new BasicDBObject ("features.Area",851 );

                // DBObject query1 = query.append(condition2);
                // DBObject query1 = query.append(condition1).append(condition2);
                // DBObject query1 = qudery.append(  "features.Perimeter", new BasicDBObject().append("$gt", 120).append("$lte", 122)   ).append(  "features.Area",851   );
                // cursor = coll.find(query);

                // DBObject matchValues = Mongo.prepareQuery(params);



                // query = new BasicDBObject("j", new BasicDBObject("$ne", 3))
                //         .append("k", new BasicDBObject("$gt", 10));
                //
                // cursor = coll.find(query);



                System.out.println("getDataRDD_call_2");

                while(cursor.hasNext()) {

                        Object data = extractDataMethod.invoke(dataClass, cursor.next());
                        if (data != null) {
                                /**
                                 * The only difference between ReadMongoSplit and MongoStream
                                 * Instead of keeping read data in List and returning in an RDD
                                 * store the read data as a stream
                                 */

                                System.out.println("read from mongodb and store");

                                store(((DCObject)data));
                                //objectsRead.add(1);
                        }
                        else
                                System.out.println("[MongoStream] Data is NULL");
                }
        }catch(Exception e) {
                e.printStackTrace();
                restart("[MongoStream] Exception while reading data", e);
        }
        finally {
                if (cursor != null)
                        cursor.close();
                if (mongoClient != null)
                        mongoClient.close();
        }
}
}
// private void receive(){
//  try{

//      //JavaRDD<DCObject> objRDD = mongoIn.getDataRDD(params).cache();
//      //long count = objRDD.count();
//      //objectsRead.add((int)count); // FIX THIS ...

//      //System.out.println("Stream size: " + count);
//      //System.out.println("Accum: " + objectsRead.value());


//      for (int i=0; i<this.count; ++i){
//        store("Test value " + i);

//        objectsRead.add(1);

//        System.out.println(objectsRead.value());

//        Thread.sleep(500);
//      }
//  }catch(Exception e){
//      restart("Some exception in custom receiver", e);
//  }

// }

}
