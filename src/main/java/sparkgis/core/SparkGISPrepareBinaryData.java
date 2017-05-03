package sparkgis.core;

/* Java imports */
import java.lang.Math;
import java.io.Serializable;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
/* JTS imports */
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.io.ParseException;
/* Local imports */
import sparkgis.data.Tile;
import sparkgis.data.SpatialObject;
import sparkgis.data.BinaryDataConfig;
import sparkgis.coordinator.SparkGISJobConf;

public class SparkGISPrepareBinaryData implements Serializable
{
    private final BinaryDataConfig dataConfig;
    
    public SparkGISPrepareBinaryData(String dataName)
    {
	/* initialize configuration */
	dataConfig = new BinaryDataConfig(dataName);
    }
    
    /**
     * Loading data is a two step process
     * Step-1: Generate tiles from data and build index
     * Step-2: Using index, map original data to physical partition tiles
     */
    public BinaryDataConfig execute(JavaRDD<byte[]> spatialData){
	dataConfig.originalData = spatialData;

	/*
	 * MBB Extraction
	 * Input:  Tab Seperated Data from Job1
	 * Output: Tab Separated Minimum Bounding Boxes Data
	 */
	JavaRDD<Tile> mbbs = spatialData.map(new MBBExtractor())
	    .filter(new Function <Tile, Boolean>(){
		    public Boolean call(Tile t) {
			return !((t.minX+t.minY+t.maxX+t.maxY) == 0);
		    }
		});
        
	extractSpaceDimensions(mbbs);
        
	return dataConfig;
    }
    
    /**
     * Input: Tab Seperated Strings: minx, miny, maxx, maxy
     * Output: Set appropriate min/max space dimension values
     */
    private void extractSpaceDimensions(JavaRDD<Tile> mbbs){
	Tile spaceDims = mbbs.reduce(new Function2<Tile, Tile, Tile>(){
		public Tile call (Tile t1, Tile t2){
		    Tile ret = new Tile();
		    ret.minX = (t1.minX < t2.minX) ? t1.minX : t2.minX;
		    ret.minY = (t1.minY < t2.minY) ? t1.minY : t2.minY;
		    ret.maxX = (t1.maxX > t2.maxX) ? t1.maxX : t2.maxX;
		    ret.maxY = (t1.maxY > t2.maxY) ? t1.maxY : t2.maxY;
		    ret.count = t1.count + t2.count;
		    return ret;
		}
	    });
	 dataConfig.setSpaceDimensions(spaceDims.minX,
				       spaceDims.minY,
				       spaceDims.maxX,
				       spaceDims.maxY);
	 dataConfig.setSpaceObjects(spaceDims.count);
    }

    /**
     * Minimum Bounding Box Extraction
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
