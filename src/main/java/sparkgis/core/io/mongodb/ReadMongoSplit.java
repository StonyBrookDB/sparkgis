package sparkgis.core.io.mongodb;
/* Java imports */
import java.lang.RuntimeException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.lang.reflect.*;
/* Spark imports */
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.FlatMapFunction;
/* MongoDB imports */
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.core.data.Polygon;
import sparkgis.core.data.MongoPolygon;
/* Local datacube imports */
import datacube.data.DCObject;

public class ReadMongoSplit
    implements Serializable, FlatMapFunction<Long, Object>
{
    private final String host;
    private final int port;
    private final Map<String, Object> params;
    private final int maxSplitSize;
    
    //reflection
    private final String dataClassName;
    //private final Class<?> obj;
    //private Object dataClass;
    //private Method extractDataMethod;

    public <T> ReadMongoSplit(String host, 
			  int port, 
			  Map<String, Object> params, 
			  int maxSplitSize,
			  Class dataClass
			  ){
	if (!params.containsKey("db") || 
	    !params.containsKey("collection") //||
	    //!params.containsKey("image.caseid") || // should be moved to pia 
	    //!params.containsKey("analysis_execution_id") // should be moved to pia
	    )
	    throw new RuntimeException("[SparkGIS] Invalid filter parameters");
	this.host = host;
	this.port = port;
	this.params = params;
	this.maxSplitSize = maxSplitSize;
	
	//try{
	    this.dataClassName = dataClass.getName();
	    //
	    //Class c = Class.forName(dataClass.getName());
	    //this.dataClass = c.newInstance();
	    //this.extractDataMethod = c.getDeclaredMethod("extractData", new Class[]{DBObject.class});
	    //}catch(Exception e){e.printStackTrace();}
	    //m.Invoke()
	    //obj = dataClass;
	//obj = dataClass.newInstance();
    }    

    /** 
     * Called by Spark flatMap
     */
    //public Iterable<MongoObject> call(Long splitStart){
    //public Iterable<T> call(Long splitStart){
    public Iterable<Object> call(Long splitStart){
    	return getDataSplit(splitStart);
    }

    /**
     * Called from ReadMongoSplit
     */
    //protected List<MongoObject> getDataSplit(long start, MongoObject obj){
    //protected List<T> getDataSplit(long start){
    protected List<Object> getDataSplit(long start){

	Method extractDataMethod = null;
	Object dataClass = null;
	try{
	    Class c = Class.forName(this.dataClassName);
	    dataClass = c.newInstance();
	    extractDataMethod = c.getDeclaredMethod("extractData", new Class[]{DBObject.class});
	}catch(Exception e){e.printStackTrace();}
	
	MongoClient mongoClient = null;
	DBCursor cursor = null;
	List<Object> ret = new ArrayList<Object>();
	//List<MongoObject> ret = new ArrayList<MongoObject>();
	// if (type == Polygon.class)
	//     ret = new ArrayList<Polygon>();
	// else if (type == DCObject.class)
	//     ret = new ArrayList<DCObject>();
	
	try{
	    mongoClient = new MongoClient(host , port);
	    DB db =  mongoClient.getDB((String)params.get("db"));
	    DBCollection results = db.getCollection((String)params.get("collection"));
	    DBObject query = Mongo.prepareQuery(params);

	    if ((maxSplitSize == 0) && (start == 0))
		cursor = results.find(query);
	    else
		cursor = results.find(query).skip((int)start).limit(maxSplitSize);	
	    while(cursor.hasNext()){
		//T data = obj.extractData(cursor.next());
		//MongoObject data = obj.extractData(cursor.next());

		Object data = extractDataMethod.invoke(dataClass, cursor.next());  
		    if (data != null)
			ret.add(data);
		    else
			System.out.println("[DataCube] Data is NULL");
		// if (type == Polygon.class){
		//     Polygon data = MongoPolygon.extractData(cursor.next());
		//     if (data != null)
		// 	ret.add(data);
		// }
		// else if (type == DCObject.class){
		//     DCObject data = DCObject.extractData(cursor.next());
		//     if (data != null)
		// 	ret.add(data);
		//}
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

    
    /**
     * For MongoToHDFS. Just for convenience
     */
    public List<Polygon> getData(long splitStart){
	// NOT good practice but this code is just for convinience
	return (List<Polygon>)(Object)getDataSplit(splitStart);
    }

}
