package sparkgis.io.mongodb;
/* Java imports */
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.FlatMapFunction;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.data.Polygon;
import sparkgis.data.MongoPolygon;
/* MongoDB imports */
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;

public class ReadMongoSplit implements Serializable, FlatMapFunction<Long, Polygon>
{
    private final String host;
    private final int port;
    private final String dbName;
    private final String caseID;
    private final String algo;
    private final int maxSplitSize;
    private final String collection;
    //private final Broadcast<MongoClient> bMongoClient;

    public ReadMongoSplit(String host, int port, String dbName, String collection,String caseID, String algo, int maxSplitSize){
	this.host = host;
	this.port = port;
	this.dbName = dbName;
	this.collection = collection;
	this.caseID = caseID;
	this.algo = algo;
	this.maxSplitSize = maxSplitSize;
	//this.bMongoClient = bm;
	
    }
    
    /**
     * Called by Spark flatMap
     */
    public Iterator<Polygon> call(Long splitStart){
	return getDataSplit(caseID, algo, splitStart, maxSplitSize);
	//return new ArrayList<Polygon>();
    }

    /**
     * For MongoToHDFS. Just for convenience
     */
    public Iterator<Polygon> getData(long splitStart){
	return getDataSplit(caseID, algo, splitStart, maxSplitSize);
    }
    
    /**
     * Called from ReadMongoSplit
     * FIX: no need to pass algo, caseID, maxSplit since already set in constructor
     */
    private Iterator<Polygon> getDataSplit(String caseID, String algo, long start, int maxSplitSize){
	MongoClient mongoClient = null;//bMongoClient.value();
	DBCursor cursor = null;
	List<Polygon> ret = new ArrayList<Polygon>(); 
	try{
	    mongoClient = new MongoClient(host , port);
	    DB db =  mongoClient.getDB(dbName);
	    DBCollection results = db.getCollection(collection);
	    DBObject query = new BasicDBObject("analysis_execution_id", algo).
		append("image.caseid", caseID);
	    if ((maxSplitSize == 0) && (start == 0))
		cursor = results.find(query);
	    else
		cursor = results.find(query).skip((int)start).limit(maxSplitSize);	
	    while(cursor.hasNext()){
		Polygon pData = extractPolygon(cursor.next());
		if (pData != null)
		    ret.add(pData);
	    }
	}catch(Exception e){e.printStackTrace();}
	finally{
	    if (cursor != null)
		cursor.close();
	    if (mongoClient != null)
	     	mongoClient.close();
	}
	return ret.iterator();
    }

    /**
     * Extract polygon information from MongoDB document
     * BSON document from MongoDB has a field 'points'
     * points: (width,height) pairs separated by space
     * The retuned RawData has polygon information as 
     * POLYGON((width height,width height,width height ... ))
     */
    private Polygon extractPolygon(DBObject doc){
	DBObject imageDoc = (DBObject)doc.get("image");
		
	String[] points = ((String)doc.get("points")).split("[\\s,]+");
	
	if (points.length > 2){
	    // create and populate a new raw data polygon for this image
	    MongoPolygon pData = new MongoPolygon(
					     doc.get("_id").toString(), 
					     ((Number)imageDoc.get("heigth")).intValue(), 
					     ((Number)imageDoc.get("width")).intValue()
					     );
	    for (String pt : points){
		pData.addPolygonPoint(pt);
	    }
	    return pData;
	}	
	return null;
    }
}
