package sparkgis.core.io.mongodb;
/* Java imports */
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.lang.reflect.*;
import java.util.ArrayList;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* MongoDB imports */
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.SparkGISConfig;


public class Mongo
{
    public static void validate(final Map<String, Object> params){
	if (
	    !params.containsKey("host") ||
	    !params.containsKey("port") || 
	    !params.containsKey("db") || 
	    !params.containsKey("collection")
	    )
	    throw new RuntimeException("[SparkGIS] Missing mongodb connection parameters (host, port, db, collection)");
    }

    /**
     * @param objectsCount Total number of objects to distribute
     * @param splitSize Each distributed split size
     * @ret RDD of splits containing start pointer for each split. Each split starts reading 
     * from its start pointer into its RDD
     */
    public static JavaRDD<Long> getSplits(Long objectsCount, int splitSize){
	
    	// create list of splits
    	List<Long> splits = new ArrayList<Long>();
    	for (long i=0; i<=objectsCount; i+=splitSize)
    	    splits.add(i);
    	// distribute splits among nodes
    	JavaRDD<Long> splitsRDD = SparkGIS.sc.parallelize(splits);
    	return splitsRDD;
    }

    /**
     * @param params Query paramters
     * @param start
     * @param maxSplitSize 
     * @param mongoClient MongoDB instance to use for querying (to read data from)
     * @ret MongoDB cursor with query data. NOTE: User is responsible for closing the cursor by calling 'cursor.close()'
     */
    public static DBCursor getDataSplit(Map<String, Object> params,
					Long start,
					int maxSplitSize,
					MongoClient mongoClient
					){
	String host = (String)params.get("host");
	int port = Integer.parseInt((String)params.get("port"));
	
	DBCursor cursor = null;
	List<Object> ret = new ArrayList<Object>();
	
	try{
	    DB db =  mongoClient.getDB((String)params.get("db"));
	    DBCollection results = db.getCollection((String)params.get("collection"));
	    DBObject query = Mongo.prepareQuery(params);
	    
	    if ((maxSplitSize == 0) && (start == 0))
		cursor = results.find(query);
	    else
		cursor = results.find(query).skip(start.intValue()).limit(maxSplitSize);	
	    //cursor = results.find(query).skip((int)start).limit(maxSplitSize);
	}catch(Exception e){e.printStackTrace();}
	
	return cursor;
    }

    /**
     * @param params Query paramters
     * @param mongoClient MongoDB instance to use for querying
     * @ret Based the total number of objects matching the specified query
     */
    public static long getObjectsCount(final Map<String, Object> params, MongoClient mongoClient){

	Mongo.validate(params);
	    
	final String host = (String)params.get("host");
	final int port = Integer.parseInt((String)params.get("port"));
	final String dbName = (String)params.get("db");
	final String collection = (String)params.get("collection");

	long count = -1;
	DBCursor cursor = null;
	try{
	    if (mongoClient == null)
		mongoClient = new MongoClient(host , port);
	    DB db =  mongoClient.getDB(dbName);
	    DBCollection results = db.getCollection(collection);
	    BasicDBObject query = Mongo.prepareQuery(params);
	    cursor = results.find(query);
	    count = cursor.count();

	    System.out.println("[Mongo] getObjectsCount: " + count);
	    
	}catch(Exception e){e.printStackTrace();}
	finally{
	    if (cursor != null)
		cursor.close();
	    if (mongoClient != null)
	     	mongoClient.close();
	}
	return count;
    }
    
    /**
     * prepare mongoDB query from all keys passed in Map
     * Note: Currently only supports equality operators
     */
    public static BasicDBObject prepareQuery(final Map<String, Object> params){
	BasicDBObject query = new BasicDBObject();
	// append all parameters for mongoDB query
	Iterator iterator = params.entrySet().iterator();
		
	while(iterator.hasNext()) {
	    Map.Entry entry = (Map.Entry)iterator.next();
	    if (entry.getKey().equals("host") ||
		entry.getKey().equals("port") ||
		entry.getKey().equals("db") ||
		entry.getKey().equals("collection"))
		continue;

	    String key = (String)entry.getKey();
	    Object value = entry.getValue();
	    if (key.equals("or") && (value instanceof Map)){
		@SuppressWarnings("unchecked")
		BasicDBList or = getClauses((Map<String, Object>)value, false);
		query.put("$or", or);
	    }
	    else
		query.append(key, (String)entry.getValue());
	}

	//System.out.println(query);

	return query;
    }
    
    public static BasicDBList getClauses(Map<String, Object> params, boolean replace){
	
	BasicDBList clauses = new BasicDBList();
	
	Iterator iterator = params.entrySet().iterator();
	while(iterator.hasNext()) {
	    Map.Entry entry = (Map.Entry)iterator.next();
	    if (entry.getKey().equals("db") || entry.getKey().equals("collection"))
		continue;
	   
	    String key = (String)entry.getKey();
	    if (replace){
		key = key.replaceAll("\\.", "__");
	    }
	    Object value = entry.getValue();
	    
	    if (value instanceof Map){
		System.out.println("TODO: Nested clauses");
	    }
	    else if (value instanceof List){
		@SuppressWarnings("unchecked")
		List<Object> str = (List<Object>)value;
		for (Object s:str){
		    DBObject clause = new BasicDBObject(key, (String)s);
		    clauses.add(clause);
		}
	    }
	    else if (value instanceof String){
		DBObject clause = new BasicDBObject(key, (String) value);
		clauses.add(clause);
	    }
	}
	return clauses;
    }
    
    //public abstract T extractData(DBObject doc);
}
