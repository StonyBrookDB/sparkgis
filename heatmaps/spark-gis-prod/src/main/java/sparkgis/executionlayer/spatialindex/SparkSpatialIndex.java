package sparkgis.executionlayer.spatialindex;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
/* JTS imports */
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory; 
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.io.ParseException;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.data.Tile;

public class SparkSpatialIndex implements Serializable{
    
    private final String shapeBegin = "POLYGON((";
    private final String shapeEnd = "))";
    private final String COMMA = ",";
    private final String SPACE = " ";
    private final String DASH = "-";
    private final String BAR = "|";
    
    private STRtree spidx;
    /**
     * Two-stage process
     * 1: Generate tiles from partitions
     * 2: Build index from tiles
     * Input: Tab Seperated list of Strings: id, minx, miny, maxx, maxy
     */
    public void build(List<Tile> partitions){
	
	GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 0);
	WKTReader reader = new WKTReader(gf);
	spidx = new STRtree();

	for (Tile tile:partitions){
	    if (tile != null){		
		String geomString = shapeBegin + tile.minX + SPACE + tile.minY + COMMA
		    + tile.minX + SPACE + tile.maxY + COMMA
		    + tile.maxX + SPACE + tile.maxY + COMMA
		    + tile.maxX + SPACE + tile.minY + COMMA
		    + tile.minX + SPACE + tile.minY + shapeEnd;		
		/* create tile geometry */
		try{
		    Geometry geom = reader.read(geomString);
		    /* 
		     * IndexedGeometry.java object with key and geometry
		     * key can be used later when doing query and getting intersecting polygon from index: Job5
		     */
		    IndexedGeometry iGeom = new IndexedGeometry(tile.tileID, geom);
		    /* add to index */
		    spidx.insert(geom.getEnvelopeInternal(), iGeom);
		}catch (ParseException e) {e.printStackTrace();}
	    }
	}
    }
    /**
     * Build index from list of strings
     */
    public void build(Iterable<String> inData, int geomid){
	GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 0);
	WKTReader reader = new WKTReader(gf);
	spidx = new STRtree();
	
	int id = 0;
	for (String data : inData){
	    try{
		String[] fields = data.split(String.valueOf(SparkGIS.TAB));
		Geometry geom = reader.read(fields[geomid]);
		IndexedGeometry iGeom = new IndexedGeometry(++id, geom);
		/* add to index */
		spidx.insert(geom.getEnvelopeInternal(), iGeom);
	    }catch(ParseException e){e.printStackTrace();}
	}
    }
    
    /**
     * @param polygonString Polygon information string 
     * @return List of IDs of index tiles overlapped by this polygon
     */
    public List<Long> getIntersectingIndexTiles(String polygonString){
	List<Long> tileIDs = new ArrayList<Long>();
	try{
	    /* create geometry for this polygon */
	    WKTReader reader = new WKTReader();
	    Geometry geometry = reader.read(polygonString);
	    Envelope env = geometry.getEnvelopeInternal();
	    /* get all intersecting polygons from index for this geometry */
	    if (env != null){
		List<?> list = spidx.query(env);
		
		for (Object o : list){
		    IndexedGeometry iGeom = (IndexedGeometry) o;		    
		    tileIDs.add(iGeom.getKey());
		}
	    }
	}catch (ParseException e) {
	    e.printStackTrace();
	}
	return tileIDs;
    }
}
