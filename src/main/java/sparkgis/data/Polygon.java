package sparkgis.data;
/* Java imports */
import java.io.Serializable;
/**
 * Class to read BSON data from MongoDB which can then be processed by Spark
 */

public abstract class Polygon implements Serializable
{
    protected final String id;
    
    public Polygon(String id){this.id = id;}
    
    public String getId(){return id;}

    public abstract String getPolygon();
    
    @Override
    public String toString(){
	return id + "\t" + getPolygon();
    }
}
