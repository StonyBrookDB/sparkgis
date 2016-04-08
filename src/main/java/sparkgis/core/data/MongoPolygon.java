package sparkgis.core.data;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Callable;
/* MongoDB imports */
import com.mongodb.DBObject;
/* Local imports */
import sparkgis.core.io.mongodb.MongoObject;

/**
 * Class To read polygon info from MongoDB
 */
public class MongoPolygon extends Polygon implements MongoObject
{
    private final int height;
    private final int width;
    private List<Double> points;
    private String polygon = "";
    
    public MongoPolygon(){
	super(null);
	height = 0;
	width = 0;
    }
    
    public MongoPolygon(String id, int height, int width){
	super(id);
	this.height = height;//Integer.parseInt(height);
	this.width = width;//Integer.parseInt(width);
	points = new ArrayList<Double>();
    }
    /**
     * Adds this point to polygon information
     * Points are in (width, height) pairs as read from MongoDB BSON object
     */
    public void addPolygonPoint(String point){
	if ((points.size() % 2) == 0)
	    points.add(Double.parseDouble(point) * width);
	else
	    points.add(Double.parseDouble(point) * height);
    }

    /**
     * Inherited abstract method implementation
     */
    public String getPolygon(){
	if (polygon.isEmpty()){
	    polygon = "POLYGON ((";
	    int i=0;
	    for (i=0; i<(points.size()-2); i+=2)
		polygon = polygon + points.get(i) + " " + points.get(i+1) + ",";
	    polygon = polygon + points.get(i) + " " + points.get(i+1) + "))";	     
	}
	return polygon;
    }


    /**
     * Called from ReadMongoSplit
     *
     * Extract polygon information from MongoDB document
     * BSON document from MongoDB has a field 'points'
     * points: (width,height) pairs separated by space
     * The retuned RawData has polygon information as 
     * POLYGON((width height,width height,width height ... ))
     */
    public Polygon extractData(DBObject doc){
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
