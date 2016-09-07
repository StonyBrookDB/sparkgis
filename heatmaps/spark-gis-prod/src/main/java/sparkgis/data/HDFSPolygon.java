package sparkgis.data;
/**
 * Class To read polygon info from HDFS
 */
public class HDFSPolygon extends Polygon
{
    private final String polygon;

    public HDFSPolygon(String id, String polygon){
	super(id);
	this.polygon = polygon;
    }
    
    /**
     * Inherited abstract method implementation
     */
    public String getPolygon(){return polygon;}
}
