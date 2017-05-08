package sparkgis.data;
/* Java imports */
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
/* JTS imports */
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.io.ParseException;

public class SpatialObjectDataConfig extends DataConfig<SpatialObject> implements Serializable
{
    private  JavaRDD<SpatialObject> originalData;

    public SpatialObjectDataConfig(String caseID){
	super(caseID);
    }
    public SpatialObjectDataConfig(String caseID, JavaRDD<SpatialObject> data){
	super(caseID);
	this.originalData = data;
    }

    @Override
    public JavaRDD<SpatialObject> getData(){return originalData;}
    @Override
    public void setData(JavaRDD<SpatialObject> data){this.originalData = data;}
    @Override
    protected JavaRDD<Tile> extractMBBs(){
	return originalData.map(new MBBExtractor())
	    .filter(new Function <Tile, Boolean>(){
		    public Boolean call(Tile t) {
			return !((t.minX+t.minY+t.maxX+t.maxY) == 0);
		    }
		});
    }

    /**
     * Minimum Bounding Box Extraction
     */
    class MBBExtractor implements Function<SpatialObject, Tile>{
	public Tile call(SpatialObject s){
	    Tile ret = new Tile();
	    try{
		WKTReader reader = new WKTReader();
		Geometry geometry = reader.read(s.getSpatialData());
		Envelope env = geometry.getEnvelopeInternal();
		
		if (env != null){
		    ret.minX = env.getMinX();
		    ret.minY = env.getMinY();
		    ret.maxX = env.getMaxX();
		    ret.maxY = env.getMaxY();
		}
	    }catch (ParseException e) {e.printStackTrace();}
	
	    return ret;
	}
    }
}
