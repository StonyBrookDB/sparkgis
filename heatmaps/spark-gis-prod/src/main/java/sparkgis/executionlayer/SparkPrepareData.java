package sparkgis.executionlayer;

/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.lang.Math;
import java.io.Serializable;
import java.util.Iterator;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
/* JTS imports */
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.io.ParseException;
/* Local imports */
//import sparkgis.SparkGIS;
import sparkgis.data.Tile;
import sparkgis.data.Polygon;
import sparkgis.data.DataConfig;
import sparkgis.stats.Profile;
import sparkgis.executionlayer.spatialindex.SparkSpatialIndex;

public class SparkPrepareData implements Serializable
{
    // Indexing 
    //private final SparkSpatialIndex ssidx;    
    private final DataConfig dataConfig;
    
    public SparkPrepareData(String imageName)
    {
	//this.imageName = imageName;
	/* initialize configuration */
	dataConfig = new DataConfig(imageName);
	/* initialize SparkSpatialIndex. Index built in 'loadStep1()' */
	//ssidx = new SparkSpatialIndex();
    }
    
    /**
     * Loading data is a two step process
     * Step-1: Generate tiles from data and build index
     * Step-2: Using index, map original data to physical partition tiles
     */
    public DataConfig execute(JavaRDD<Polygon> polygons){
	dataConfig.originalData = polygons;
	/* pass data to step-1 for further processing */
	loadStep1(polygons);
	// // move on to step-2
	// loadStep2(sampledTSV);
	//dataConfig.dataPrepTime = System.nanoTime() - start;

	return dataConfig;
    }

    /**
     * Step-1 is further divided into 6 parts
     * 1) Extract MBBs from input data
     * 2) Extract space dimensions from data
     * 3) Normalize extracted MBBs using space dimensions. (For BSP only???)
     * 4) Generate partitions using Fixed-Grid Partitioner
     * 5) Denormalize partitions
     * 6) Use denormalized partitions to build index
     */
    private void loadStep1(JavaRDD<Polygon> polygonsData){
	
	/************ Stage-2.1: MBB Extraction ************/
	/*
	 * Input:  Tab Seperated Data from Job1
	 * Output: Tab Separated Minimum Bounding Boxes Data
	 */
	JavaRDD<Tile> mbbs = polygonsData.map(new MBBExtractor())
	    .filter(new Function <Tile, Boolean>(){
		    public Boolean call(Tile t) {
			return !((t.minX+t.minY+t.maxX+t.maxY) == 0);
		    }
		});
	/**************** End Stage-2.1 ********************/
	
	/****** Stage-2.2: Space dimensions retreival ******/
	extractSpaceDimensions(mbbs);	
	/**************** End Stage-2.2 ********************/
    }
    
    /************************** Helper Methods **************************/
        
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
	 dataConfig.setSpaceDimensions(spaceDims.minX, spaceDims.minY, spaceDims.maxX, spaceDims.maxY);
	 dataConfig.setSpaceObjects(spaceDims.count);
    }
    /************************* Spark Jobs ********************************/

    /**
     * Job2: Minimum Bounding Box Extraction
     */
    class MBBExtractor implements Function<Polygon, Tile>{
	public Tile call(Polygon p){
	    Tile ret = new Tile();
	    try{
		WKTReader reader = new WKTReader();
		Geometry geometry = reader.read(p.getPolygon());
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

    // /**
    //  * Map orginal data to paritions using index from step-1
    //  */
    // public void loadStep2(JavaRDD<String> rawData){
    // 	/***** Stage-3.2: Data to physical partitions *****/
    // 	System.out.print("\tStage-3.2: Starting 'Map data to physical partitions' .......... ");
    // 	dataConfig.mappedPartitions = rawData.flatMapToPair(new PartitionMapper(dataConfig.getGeomid()));
    //     System.out.println("Done!");
    // 	/**************** End Stage-3.2 ********************/	
    // }
    
    
    // /**
    //  * Denormalize partitions
    //  */
    // private List<Tile> denormalize(List<Tile> partitions){
    // 	double spaceXSpan = dataConfig.getSpanX();
    // 	double spaceYSpan = dataConfig.getSpanY();
    // 	double dataMinX = dataConfig.getMinX();
    // 	double dataMinY = dataConfig.getMinY();
    // 	double dataMaxX = dataConfig.getMaxX();
    // 	double dataMaxY = dataConfig.getMaxY();
	
    // 	List<Tile> denormPartition = new ArrayList<Tile>();
    // 	for (Tile pTile : partitions){
    // 	    pTile.minX = pTile.minX * spaceXSpan + dataMinX;
    // 	    pTile.minY = pTile.minY * spaceYSpan + dataMinY;
    // 	    pTile.maxX = pTile.maxX * spaceXSpan + dataMinX;
    // 	    pTile.maxY = pTile.maxY * spaceYSpan + dataMinY;
	    
    // 	    denormPartition.add(pTile);
    // 	}
    // 	return denormPartition;
    // }
    
    
    // /**
    //  * Job5: Map all objects to physical partitions using index
    //  * Input: tsv raw data (spatial object)
    //  * Output: Zero or more Tuple<key, value> where 
    //  *         key: ID of intersecting polygon in index
    //  *         value: tsv geometry of this spatial object
    //  *         Can be mutiple for spatial objects that lie on boundary of indexed polygon
    //  */
    // class PartitionMapper implements PairFlatMapFunction<String, Long, String>{
    // 	private final int geomid;	
    // 	public PartitionMapper(int geomid){this.geomid = geomid;}
    // 	public Iterable<Tuple2<Long, String>> call (String line){
    // 	    // String[] fields = line.split(String.valueOf(SparkGIS.TAB));
    // 	    // //Initalize return array of tuples
    // 	    // List <Tuple2<Long, String>> ret = new ArrayList<Tuple2<Long, String>>();
	    
    // 	    // if (fields.length >= 2){
    // 	    // 	List<Long> tileIDs = ssidx.getIntersectingIndexTiles(fields[geomid]);
    // 	    // 	for (long id : tileIDs){
    // 	    // 	    Tuple2<Long, String> t = new Tuple2<Long, String>(id, line);
    // 	    // 	    ret.add(t);
    // 	    // 	}
    // 	    // }
    // 	    // return ret;
    // 	    List <Tuple2<Long, String>> ret = new ArrayList<Tuple2<Long, String>>();
    // 	    ret.add(new Tuple2<Long, String>(new Long(1), line));
    // 	    return ret;
    // 	}
    // }
    
    /************************** Getter/Setter ****************************/

    // public void setDelimiter(char delimiter) {this.origDelimiter = delimiter;}
    // public void setRatio (int ratio) {this.ratio = ratio;}
}
