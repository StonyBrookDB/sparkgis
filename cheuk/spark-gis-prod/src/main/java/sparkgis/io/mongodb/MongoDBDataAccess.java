package sparkgis.io.mongodb;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
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
import sparkgis.io.ISparkGISIO;
import sparkgis.data.Polygon;
import sparkgis.data.TileStats;
import sparkgis.data.MongoPolygon;
import sparkgis.SparkGISConfig;

public class MongoDBDataAccess implements ISparkGISIO
{    
    // MongoDB: BMI Cluster
    
    
 
    
    // private String host = "nfs011";
    // private int port = 27015;
    // private String dbName = "u24_segmentation";
    // private String collection ="results";

//private String host = SparkGISConfig.mongoHost;
   // private int port = SparkGISConfig.mongoPort;
    //private String dbName = SparkGISConfig.mongoDB;

 private String dbName = "";
    private String collection ="";

      private String host = "";
    private int port = 0;
    private String inputdbName = "";
    private String inputdbCollection ="";
     private String outputdbName = "";
    private String outputdbCollection ="";




    /**
     * Default constructor for BMI MongoDB
     */
    public MongoDBDataAccess(){

		this.host = SparkGISConfig.mongoHost;
    	this.port = SparkGISConfig.mongoPort;
    	this.inputdbName = SparkGISConfig.input_mongoDB;
    	this.inputdbCollection = SparkGISConfig.input_collection_name;
    	this.outputdbName =   SparkGISConfig.output_mongoDB;
    	this.outputdbCollection = SparkGISConfig.output_collection_name;

    	  

    }
    /**
     * @param host MongoDB host name
     * @param port MongoDB server port number
     * @param dbName MongoDB database to use
     */
    public MongoDBDataAccess(String host, int port, String dbName,String collection){
    	this.host = host;
    	this.port = port;
    	this.dbName = dbName;
    	this.collection = collection;
    }
    
    public JavaRDD<Polygon> getPolygonsRDD(String caseID, String algo){
	long objectsCount = 0;
	try{
    	    final MongoClient mongoClient = new MongoClient(host , port);
	    objectsCount = getObjectsCount(caseID, algo, mongoClient);
	}catch(Exception e){System.out.println(e.toString());}

	if (objectsCount == 0){
	    throw new RuntimeException("No object found for caseID:" + caseID + ", algorithm: " + algo);
	}
	
	final int nSplits = SparkGIS.sc.defaultParallelism();
	final int splitSize = (int)objectsCount/nSplits;
	
	// create list of splits
	List<Long> splits = new ArrayList<Long>();
	for (long i=0; i<=objectsCount; i+=splitSize)
	    splits.add(i);
	// distribute splits among nodes
	JavaRDD<Long> splitsRDD = SparkGIS.sc.parallelize(splits);
	return splitsRDD.flatMap(new ReadMongoSplit(host, port, inputdbName, inputdbCollection,caseID, algo, splitSize));
    }

    /**
     * Sequential (Non-Distributed) write results to MongoDB
     *    1. Collect results from all nodes
     *    2. Upload results to MongoDB
     */
    public String writeTileStats(JavaRDD<TileStats> result, String... args){

System.out.println("sadsadasd");
	String caseID = args[0];
	String analysis_exe_id = args[1];
	String title = args[2];
	String result_analysis_exe_id = args[3];
 String jobId = args[4];
 
 System.out.println("jobId:    "+jobId);
	
	List<TileStats> resultList = result.collect();
	try{
    	    final MongoClient mongoClient = new MongoClient(host , port);
	    DB db =  mongoClient.getDB(outputdbName);

	    System.out.println("helloworld!!!!!!!!!!!");

	     System.out.println(outputdbCollection);
	    
	    DBCollection results = db.getCollection(outputdbCollection);
         // DBCollection ad_hoc_results = db.getCollection(SparkGISConfig.collection_name);
	    
	    BasicDBObject query = new BasicDBObject();
	    query.put("analysis_execution_id", analysis_exe_id); 
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
		    .append("image", image)
        .append("jobId", jobId);
		
		results.insert(wdoc);
   // ad_hoc_results.insert(wdoc);
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
    
    private long getObjectsCount(final String caseID, final String algo, final MongoClient mongoClient){
	long count = -1;
	DBCursor cursor = null;
	try{
		System.out.println("test: inputdbName:	"+inputdbName);
		System.out.println("test: inputdbCollection:	"+inputdbCollection);
	    DB db =  mongoClient.getDB(inputdbName);
	    DBCollection results = db.getCollection(inputdbCollection);
	    DBObject query = new BasicDBObject("analysis_execution_id", algo).
		append("image.caseid", caseID);
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
