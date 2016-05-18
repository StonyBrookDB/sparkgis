package sparkgis.core.io.mongodb;
/* Java imports */
import java.util.Map;
import java.util.List;
import java.util.Iterator;
/* MongoDB imports */
import com.mongodb.DBObject;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
/* Local imports */
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
     * prepare mongoDB query from all keys passed in Map
     * Note: Currently only supports equality operators
     */
    public static BasicDBObject prepareQuery(final Map<String, Object> params){
	BasicDBObject query = new BasicDBObject();
	// append all parameters for mongoDB query
	Iterator iterator = params.entrySet().iterator();
		
	while(iterator.hasNext()) {
	    Map.Entry entry = (Map.Entry)iterator.next();
	    if (entry.getKey().equals("db") || entry.getKey().equals("collection"))
		continue;

	    String key = (String)entry.getKey();
	    Object value = entry.getValue();
	    if (key.equals("or") && (value instanceof Map)){
		BasicDBList or = getClauses((Map<String, Object>)value, false);
		query.put("$or", or);
	    }
	    else
		query.append(key, (String)entry.getValue());
	}

	System.out.println(query);

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
