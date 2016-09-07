package sparkgis.data;
import java.util.List;
import java.util.ArrayList;

/**
 * Class To read polygon info from MongoDB
 */
public class MongoPolygon extends Polygon
{
    private final int height;
    private final int width;
    private List<Double> points;
    private String polygon = "";
    
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
}
