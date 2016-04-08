package sparkgis.core.io.mongodb;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
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

public class MongoDBDataAccess 
    implements ISparkGISIO
{    
    private final String host = SparkGISConfig.mongoHost;
    private final int port = SparkGISConfig.mongoPort;
    // FIX: MUST BE REMOVED ...
    private final String dbName = SparkGISConfig.mongoDB;

    /**
     * Default constructor for BMI MongoDB
     */
    public MongoDBDataAccess(){
	//super(host, port, params, maxSplitSize);
    }

    public JavaRDD getDataRDD(Map<String, Object> params){
	long objectsCount = 0;
	try{
    	    final MongoClient mongoClient = new MongoClient(host , port);
	    objectsCount = getObjectsCount(params, mongoClient);
	}catch(Exception e){System.out.println(e.toString());}
	
	final int nSplits = SparkGIS.sc.defaultParallelism();
	final int splitSize = (int)objectsCount/nSplits;
	
	JavaRDD<Long> splits = getSplits(objectsCount, splitSize);
	//return splits.flatMap(new ReadMongoSplit<MongoPolygon>(host, port, params, splitSize, MongoPolygon.class));
	return splits.flatMap(new ReadMongoSplit(host, port, params, splitSize, MongoPolygon.class));
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
	return (JavaRDD<Polygon>)getDataRDD(filterParams);
    }
    
    //private JavaRDD getRDD(Map<String, String> filterParams){
    public JavaRDD<Long> getSplits(Long objectsCount, int splitSize){
	
	//final int nSplits = SparkGIS.sc.defaultParallelism();
	//final int splitSize = (int)objectsCount/nSplits;
	
	// create list of splits
	List<Long> splits = new ArrayList<Long>();
	for (long i=0; i<=objectsCount; i+=splitSize)
	    splits.add(i);
	// distribute splits among nodes
	JavaRDD<Long> splitsRDD = SparkGIS.sc.parallelize(splits);
	return splitsRDD;
	//return splitsRDD.flatMap(new ReadMongoSplit(host, port, filterParams, splitSize));
    }
    
    public void writeRDD(JavaRDD data, String outPath){
	// TODO
    }

    /**
     * Sequential (Non-Distributed) write results to MongoDB
     *    1. Collect results from all nodes
     *    2. Upload results to MongoDB
     */
    public String writeTileStats(JavaRDD<TileStats> result, String... args){
	String caseID = args[0];
	String orig_analysis_exe_id = args[1];
	String title = args[2];
	String result_analysis_exe_id = args[3];

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

    /*********************************** Private methods ****************************/
    
    public long getObjectsCount(final Map<String, Object> params, MongoClient mongoClient){
	if (
	    !params.containsKey("db") || 
	    !params.containsKey("collection")
	    )
	    throw new RuntimeException("[SparkGIS] Missing required parameters (db OR collection)");
	final String dbName = (String)params.get("db");
	final String collection = (String)params.get("collection");

	long count = -1;
	DBCursor cursor = null;
	try{
	    if (mongoClient == null)
		mongoClient = new MongoClient(host , port);
	    DB db =  mongoClient.getDB(dbName); // FIX: remove class variable
	    DBCollection results = db.getCollection(collection);
	    BasicDBObject query = Mongo.prepareQuery(params);
	    cursor = results.find(query);
	    count = cursor.count();
	}catch(Exception e){e.printStackTrace();}
	finally{
	    if (cursor != null)
		cursor.close();
	    if (mongoClient != null)
	     	mongoClient.close();
	}
	return count;
    }
}
