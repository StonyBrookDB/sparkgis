package datacube.data;
/* Java imports */
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.Serializable;
import java.util.concurrent.Callable;
/* MongoDB imports */
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;

//import com.mongodb.Block;
//import com.mongodb.client.AggregateIterable;
//import org.bson.Document;
//import static java.util.Arrays.asList;
import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBList;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
/* SparkGIS imports */
import sparkgis.core.io.mongodb.MongoObject;
/* Local imports */
import datacube.data.Property;
import datacube.data.DCDimension;
import datacube.data.PropertyName;

/**
 * Class to read in data from source
 * Features will be translated to Dimensions while creating datacube
 */
public class DCObject implements Serializable, MongoObject
{
    private final String id;
    public List<Property> props;
    
    public DCObject(){
	id = "";
	props = new ArrayList<Property>();
    }
    
    public DCObject(String id){
	this.id = id;
	this.props = new ArrayList<Property>();
    }

    public String getID(){return this.id;}
    
    
    public void addProperty(Property p){
	props.add(p);
    }

    public String toString(){
	String ret = id;
	System.out.println("Num of Props: " + props.size());
	for (Property p:props){
	    ret += "\t"+p.getNameStr()+": "+p.getValue() + "\n";
	}
	return ret;
    }
    
    /**
     * Called from ReadMongoSplit
     */ 
    public DCObject extractData(DBObject doc){
	
	DCObject obj = new DCObject(doc.get("_id").toString());
	// FIX THIS: caseid can also be a dimension
	//obj.addProperty(new Property(Property.PropertyName.CASEID, 0));

	BasicDBObject featuresDoc = (BasicDBObject)doc.get("features");
	
	// Iterator iterator = featuresDoc.entrySet().iterator();
	// while(iterator.hasNext()) {
	//     Map.Entry entry = (Map.Entry)iterator.next();
	//     //query.append((String)entry.getKey(), (String)entry.getValue());
	// }
	
	// read in all features in memory
	List<PropertyName> allProps = Arrays.asList(PropertyName.values());
	
	for(PropertyName pName:allProps){
	    if (pName.value.equals("image.caseid"))
		obj.addProperty(new StringProperty(pName, featuresDoc.getString(pName.value)));
		//obj.addProperty(new StringProperty(pName, featuresDoc.getString(pName.value)));
	    else
		obj.addProperty(new DoubleProperty(pName, featuresDoc.getDouble(pName.value)));
	}
	
	return obj;
    }
}
