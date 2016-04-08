package sparkgis.core.io.mongodb;
/* Java imports */
//import java.util.Map;
//import java.util.Iterator;
/* MongoDB imports */
import com.mongodb.DBObject;
/* Local imports */
//import sparkgis.SparkGISConfig;

//public interface MongoObject<T extends MongoObject<T>>  
public interface MongoObject
{    
    public Object extractData(DBObject doc);
}
