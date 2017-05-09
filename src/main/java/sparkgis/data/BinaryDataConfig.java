package sparkgis.data;
/* Java imports */
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
/* JTS imports */
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.io.ParseException;

/**
 * Contains all data components required for querying. Populated and returned by SparkPrepareData
 */
public class BinaryDataConfig extends DataConfig<byte[]> implements Serializable
{
    private JavaRDD<byte[]> originalData;
    
    public BinaryDataConfig(String caseID){
	super(caseID);
    }
    public BinaryDataConfig(String caseID, JavaRDD<byte[]> data){
	super(caseID);
	this.originalData = data;
    }

    @Override
    public JavaRDD<byte[]> getData(){return originalData;}
    @Override
    public void setData(JavaRDD<byte[]> data){this.originalData = data;}
    
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
     * Minimum Bounding Box Extraction for binary spatial data
     */
    class MBBExtractor implements Function<byte[], Tile>{
	public Tile call(byte[] s){
	    Tile ret = new Tile();
	    try{
		WKBReader reader = new WKBReader();
		Geometry geometry = reader.read(s);
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
