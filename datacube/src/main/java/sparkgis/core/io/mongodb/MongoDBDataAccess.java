package sparkgis.core.io.mongodb;
/* Java imports */
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.io.Serializable;
import java.lang.RuntimeException;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.FlatMapFunction;
/* MongoDB imports */
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.SparkGISConfig;
import sparkgis.core.io.ISparkGISIO;
import sparkgis.core.data.Polygon;
import sparkgis.core.data.TileStats;
import sparkgis.core.data.MongoPolygon;
import sparkgis.core.enums.Delimiter;
/* Local datacube imports*/
import datacube.data.DCObject;

/* Mongo through Hadoop connector */
// import org.bson.BSONObject;
// import org.bson.BasicBSONObject;
//import org.apache.hadoop.conf.Configuration;

public class MongoDBDataAccess 
    implements ISparkGISIO
{    
    public MongoDBDataAccess(){}

    public JavaRDD getDataRDD(Map<String, Object> params, String dataClassName){
	
<<<<<<< HEAD
	Mongo.validate(params);
	
	final String host = (String)params.get("host");
	final int port = Integer.parseInt((String)params.get("port"));
	
=======
  System.out.println("111");
 
	Mongo.validate(params);
	
 System.out.println("222");
 
 
	final String host = (String)params.get("host");
	final int port = Integer.parseInt((String)params.get("port"));
 
 //final int portNum = (Integer)params.get("port");
 //final String port = String.valueOf(portNum);
 
 //final int port   = (Integer)params.get("port");
	
 System.out.println("test host"+host);
  System.out.println("test host"+port);
  
  String db =  (String)params.get("db");
  
  String collection =  (String)params.get("collection");
  String  id =  (String)params.get("analysis_execution_id");
  
  String image =  (String)params.get("image.caseid");
  
  
  
  System.out.println("db:"+db);
  System.out.println("collection:"+collection);
  System.out.println("analysis_execution_id:"+id);
  System.out.println("image.caseid:"+image);
  
  
  
   
 
>>>>>>> e2309c874cc88cac1cf2060fe64c898fdefb3ad7
	long objectsCount = 0;
	try{
    	    final MongoClient mongoClient = new MongoClient(host , port);
	    objectsCount = Mongo.getObjectsCount(params, mongoClient);
<<<<<<< HEAD
	}catch(Exception e){System.out.println(e.toString());}
=======
         
         System.out.println(params.toString());
	}catch(Exception e){
  
  System.out.println("error here");
  System.out.println(e.toString());}
>>>>>>> e2309c874cc88cac1cf2060fe64c898fdefb3ad7

	if (objectsCount == 0)
	    throw new RuntimeException("[MongoDBDataAccess] Query Objects count: " +
				       objectsCount);
	
	final int nSplits = SparkGIS.sc.defaultParallelism();
	final int splitSize = (int)objectsCount/nSplits;

	System.out.println("[MongoDBDataAccess] Total Objects: " +
			   objectsCount +
			   ", Total Splits: " +
			   nSplits +
			   ", Split Size: " +
			   splitSize);
	
	JavaRDD<Long> splits = Mongo.getSplits(objectsCount, splitSize);
	//return splits.flatMap(new ReadMongoSplit<MongoPolygon>(host, port, params, splitSize, MongoPolygon.class));
	//return splits.flatMap(new ReadMongoSplit(params, splitSize, MongoPolygon.class));
	
	return splits.flatMap(new ReadMongoSplit(params, splitSize, dataClassName));
    }
    
    /**
     * @param filterParams
     *        <db, value>
     *        <collection, value>
     * Add all other parameters required to get data from specified collection e.g.
     *        <caseID, value>
     *        <algorithm, value>
     *
     */
    public JavaRDD<Polygon> getPolygonsRDD(Map<String, Object> filterParams){
	return (JavaRDD<Polygon>)getDataRDD(filterParams, MongoPolygon.class.getName());
    }

    public void writeRDD(JavaRDD data, String outPath){
	// TODO
    }

    /**
     * Sequential (Non-Distributed) write results to MongoDB for caMicroscope
     *    1. Collect results from all nodes
     *    2. Upload results to MongoDB
     */
    public String writeTileStats(JavaRDD<TileStats> result, Map<String, String> args){
	// String caseID = args[0];
	// String orig_analysis_exe_id = args[1];
	// String title = args[2];
	// String result_analysis_exe_id = args[3];

	final String host = (String)args.get("host");
	final int port = Integer.parseInt((String)args.get("port"));
	final String dbName = (String)args.get("db");
	
	final String caseID = args.get("caseid");
	final String orig_analysis_exe_id = args.get("orig_analysis_exe_id");
	final String title = args.get("title");
	final String result_analysis_exe_id = args.get("result_analysis_exe_id");
	
	List<TileStats> resultList = result.collect();
	try{
    	    final MongoClient mongoClient = new MongoClient(host , port);
	    DB db =  mongoClient.getDB(dbName);
	    DBCollection results = db.getCollection("results");
	    
	    BasicDBObject query = new BasicDBObject();
	    query.put("analysis_execution_id", orig_analysis_exe_id); 
	    query.put("image.caseid", caseID);
	    DBObject doc = results.findOne(query);
	    DBObject image = (DBObject)doc.get("image");
	    Double W = Double.parseDouble(image.get("width").toString());
	    Double H = Double.parseDouble(image.get("heigth").toString());
	    
	    for (TileStats res:resultList){
		Double stat = res.statistics;
		Integer tile_id = (int) res.tile.tileID;
		Double xmin = res.tile.minX;
		Double ymin = res.tile.minY; 
		Double xmax = res.tile.maxX; 
		Double ymax = res.tile.maxY; 
		
		ArrayList<Double> al = new ArrayList<Double>();
		al.add(xmin/W);
		al.add(ymin/H);
     
		BasicDBObject features = new BasicDBObject("Area", (xmax - xmin)*(ymax - ymin))
		    .append("Metric", stat)
		    .append("MetricType", res.type);

		BasicDBObject wdoc = new BasicDBObject("analysis_execution_id", result_analysis_exe_id)
		    .append("x", xmin/W)
		    .append("tile_id", tile_id)
		    .append("y", ymin/H)
		    .append("loc", al)
		    .append("w", (xmax-xmin)/W)
		    .append("h", (ymax-ymin)/H)
		    .append("normalized", new Boolean(true))
		    .append("type", "heatmap")
		    .append("color", "red")
		    .append("features", features)
		    .append("image", image);
		
		results.insert(wdoc);
	    }
	    
	    // update metadata
	    DBCollection coll_meta1 = db.getCollection("metadata");
	    DB db2 = mongoClient.getDB( "tcga_segmentation" );
	    DBCollection coll_meta2 = db2.getCollection("metadata");
	    BasicDBObject meta_doc = new BasicDBObject("analysis_execution_id", result_analysis_exe_id)
                .append("caseid", caseID)
                .append("title", title);

	    coll_meta1.insert(meta_doc);
	    coll_meta2.insert(meta_doc);
	    
	}catch(Exception e){System.out.println(e.toString());}
	return "";
    }


    /**
     * Inner class
     */
    class ReadMongoSplit
	implements Serializable, FlatMapFunction<Long, Object>
    {
	private final String host;
	private final int port;
	private final Map<String, Object> params;
	private final int maxSplitSize;
    
	//for reflection
	private final String dataClassName;

	public ReadMongoSplit(Map<String, Object> params,
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
	 * Called by Spark flatMap
	 */
	public Iterable<Object> call(Long start){

	    // Reflection to get data extraction method
	    Method extractDataMethod = null;
	    Object dataClass = null;
	    try{
		Class<?> c = Class.forName(dataClassName);
		dataClass = c.newInstance();
		extractDataMethod = c.getDeclaredMethod("extractData", new Class[]{DBObject.class});
	    }catch(Exception e){e.printStackTrace();}

	    MongoClient mongoClient = null;
	    List<Object> ret = new ArrayList<Object>();
	    DBCursor cursor = null;
	    
	    try{
		mongoClient = new MongoClient(host , port);
		cursor = Mongo.getDataSplit(params, start, maxSplitSize, mongoClient);
		
		while(cursor.hasNext()){
		    Object data = extractDataMethod.invoke(dataClass, cursor.next());  
		    if (data != null)
			ret.add(data);
		    else
			System.out.println("[ReadMongoSplit] Data is NULL");
		}
	    }catch(Exception e){e.printStackTrace();}
	    finally{
		if (cursor != null)
		    cursor.close();
		if (mongoClient != null)
		    mongoClient.close();
	    }
	    return ret;
	}

    
	// /**
	//  * For MongoToHDFS. Just for convenience
	//  */
	// public List<Polygon> getData(long splitStart){
	//     // NOT good practice but this code is just for convinience
	//     return (List<Polygon>)(Object)getDataSplit(splitStart);
	// }

    }


    /************************* Test Code ****************************/
    
    // /**
    //  * Mongo through Hadoop connector configurations
    //  * Prevous Version: Load all collection in memory prior to running any query
    //  * Current Version: 
    //  *        - mongo.input.query 'can filter data before loading in memory ???'
    //  *        - makes tooo many child processes which run out of memory
    //  */
    // public JavaPairRDD getDataRDDMongoHadoop(Map<String, Object> params){

    // 	Mongo.validate(params);
	    
    // 	if (!params.containsKey("query"))
    // 	    throw new RuntimeException("[SparkGIS] Missing mongodb query parameter");

	
    // 	final String host = (String)params.get("host");
    // 	final int port = Integer.parseInt((String)params.get("port"));
    // 	final String dbName = (String)params.get("db");
    // 	final String collection = (String)params.get("collection");
    // 	final String query = (String)params.get("query");
	
    // 	final String inputUri =
    // 	    "mongodb://" + host + ":" + port + "/" + dbName + "." + collection;

    // 	System.out.println("Input URI: " + inputUri);
	
    // 	Configuration config = new Configuration();
    // 	config.set("mongo.input.uri", inputUri);
    // 	config.set("mongo.input.query", query);
    // 	//config.set("mongo.output.uri", outputUri);

    // 	JavaPairRDD<Object, BSONObject> data =
    // 	    SparkGIS.sc.newAPIHadoopRDD(
    // 					config,
    // 					com.mongodb.hadoop.MongoInputFormat.class,
    // 					Object.class,
    // 					BSONObject.class
    // 					);

    // 	System.out.println("Data Count: " + data.count());

    // 	return data;
    // 	//return null;
    // }
}
