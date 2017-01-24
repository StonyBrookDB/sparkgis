package sparkgis.data;
/* Java imports */
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
/* Local imports */
import sparkgis.SparkGIS;

/**
 * Contains all data components required for querying. Populated and returned by SparkPrepareData
 */
public class DataConfig implements Serializable
{
    public final String caseID;
    /* 
     * geometry index in TAB separated string 
     * by default 1 (polygonID, polygonGeom)
     */
    private int geomid = 2;

    public JavaRDD<Polygon> originalData;

    /* Space stuff */
    private double dataMinX;
    private double dataMinY;
    private double dataMaxX;
    private double dataMaxY;
    private long numObjects = 0;
    
    public DataConfig(String caseID){
	this.caseID = caseID;
    }
    
    public int getGeomid() {return this.geomid;}
    public void setGeomid(int geomid) {this.geomid = geomid;}
    
    /********************* Space Stuff *******************/
    /**
     * Called from SparkPrepareData
     */
    public void setSpaceDimensions(double minX, double minY, double maxX, double maxY){
	this.dataMinX = minX;
	this.dataMinY = minY;
	this.dataMaxX = maxX;
	this.dataMaxY = maxY;
    }
    /**
     * Called from SparkPrepareData
     */
    public void setSpaceObjects(long numObjects) {
	this.numObjects = numObjects;
    }
    public double getSpanX(){
	return (this.dataMaxX - this.dataMinX);
    }
    public double getSpanY(){
	return (this.dataMaxY - this.dataMinY);
    }
    
    public double getMinX() {return this.dataMinX;}
    public double getMinY() {return this.dataMinY;}
    public double getMaxX() {return this.dataMaxX;}
    public double getMaxY() {return this.dataMaxY;}
    public long getSpaceObjects() {return this.numObjects;}

    /* Unecessary stuff */

    // private int blockSize = 67108864; // default block size = 64MB
    // private final Space dataSpace = new Space(); // corresponds to data.cfg in Hadoop-GIS
    // private List<Tile> partitionIDX;
    // public void setPartitionIDX(List<Tile> partIDX) {this.partitionIDX = partIDX;}

    //public List<Tile> getPartitionIDX() {return this.partitionIDX;}
    //public int getPartitionsCount() {return this.partitionIDX.size();}
    
    // /**
    //  * Calculate and return partition size from input size and ratio
    //  * NOTE: setSpaceObjects() must be called before calling this. Otherwise "divided by zero"
    //  * @param totalSize Input file size in bytes
    //  * @param ratio 
    //  */
    // public int getPartitionBucketSize(long totalSize, int ratio){
    // 	if (dataSpace.partitionBucketSize == -1){
    // 	    int partSize = 0;
    // 	    double avgObjSize = totalSize / dataSpace.numObjects;
    // 	    dataSpace.partitionBucketSize = (int) Math.floor((blockSize * ratio) / avgObjSize);
    // 	}
    // 	return dataSpace.partitionBucketSize;
    // }

    // public void setPartitionBucketSize(int partSize) {dataSpace.partitionBucketSize = partSize;}

    // public String getSpace(){
    // 	if (dataSpace != null)
    // 	    return dataSpace.toString();
    // 	else
    // 	    return "Configuration not set yet ...";
    // }
    
    //public int getPartitionBucketSize(){return dataSpace.partitionBucketSize;}

    // class Space implements Serializable
    // {
    // 	double dataMinX;
    // 	double dataMinY;
    // 	double dataMaxX;
    // 	double dataMaxY;
    // 	long numObjects = 0;
    // 	int partitionBucketSize = -1;
	
    // 	public String toString(){
    // 	    return "dataMinX=" + dataMinX + "\ndataMinY=" + dataMinY + "\ndataMaxX=" + dataMaxX + "\ndataMaxY=" + dataMaxY + "\nnumobjects=" + numObjects + "\npartitionsize=" + partitionBucketSize;
    // 	}
    // }
    

    /*** Can be used for debugging ***/
    // public String toString(){
    // 	String ret;
    // 	ret = "partfile.idx\n";
    // 	// if (partitionIDX != null){
    // 	//     for (Tile t : partitionIDX)
    // 	// 	ret = ret + t.toString() + "\n";
    // 	// }
    // 	ret = ret + "Geomid: " + geomid + "\n";
    // 	ret = ret + "SpanX: " + getSpanX() + "\n";
    // 	ret = ret + "SpanY: " + getSpanY() + "\n";
    // 	ret = ret + dataSpace.toString();
    // 	return ret;
    // }
}
