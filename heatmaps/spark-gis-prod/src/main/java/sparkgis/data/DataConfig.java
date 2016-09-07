package sparkgis.data;
/* Java imports */
import java.util.List;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
/* Local imports */
import sparkgis.SparkGIS;

/**
 * Contains all data components required for querying. Populated and returned by SparkPrepareData
 * Partition Index:
 *     getPartitionIDX()
 *     getPartitionsCount()
 * Distributed Minimum Bounding Boxes (MBBs): public JavaRDD<String> mbbs
 * Distributed Mapped Partitions: public JavaPairRDD<Long, String> mappedPartitions
 * Geomid:
 *     getGeomid()
 * Space Dimensions: 
 *     getMinX()
 *     getMinY()
 *     getMaxX()
 *     getMaxY()
 * Space span:
 *     getSpanX()
 *     getSpanY()
 * Space misc:
 *     getSpaceObjects()
 *     getPartitionBucketSize(int inputSize, int ratio)
 */
public class DataConfig implements Serializable
{
    private final int geomid = 2;
    private int blockSize = 67108864; // default block size = 64MB
    private List<Tile> partitionIDX;
    private final Space dataSpace = new Space(); // corresponds to data.cfg in Hadoop-GIS

    public final String caseID;    
    public JavaRDD<Polygon> originalData;

    //public long dataPrepTime;

    public DataConfig(String caseID){
	this.caseID = caseID;
    }    
    
    public void setPartitionIDX(List<Tile> partIDX) {this.partitionIDX = partIDX;}    
    public int getGeomid() {return this.geomid;}

    public List<Tile> getPartitionIDX() {return this.partitionIDX;}
    public int getPartitionsCount() {return this.partitionIDX.size();}

    public String toString(){
	String ret;
	ret = "partfile.idx\n";
	if (partitionIDX != null){
	    for (Tile t : partitionIDX)
		ret = ret + t.toString() + "\n";
	}
	ret = ret + "Geomid: " + geomid + "\n";
	ret = ret + "SpanX: " + getSpanX() + "\n";
	ret = ret + "SpanY: " + getSpanY() + "\n";
	ret = ret + dataSpace.toString();
	return ret;
    }
    
    /********************* Space Stuff *******************/
    public void setSpaceDimensions(double minX, double minY, double maxX, double maxY){
	//dataSpace = new Space();
	dataSpace.dataMinX = minX;
	dataSpace.dataMinY = minY;
	dataSpace.dataMaxX = maxX;
	dataSpace.dataMaxY = maxY;
    }
    
    public void setSpaceObjects(long numObjects) {
	this.dataSpace.numObjects = numObjects;
    }

    public void setPartitionBucketSize(int partSize) {dataSpace.partitionBucketSize = partSize;}

    public String getSpace(){
	if (dataSpace != null)
	    return dataSpace.toString();
	else
	    return "Configuration not set yet ...";
    }


    public double getSpanX(){
	return (dataSpace.dataMaxX - dataSpace.dataMinX);
    }
    public double getSpanY(){
	return (dataSpace.dataMaxY - dataSpace.dataMinY);
    }
    
    public double getMinX() {return dataSpace.dataMinX;}
    public double getMinY() {return dataSpace.dataMinY;}
    public double getMaxX() {return dataSpace.dataMaxX;}
    public double getMaxY() {return dataSpace.dataMaxY;}
    public long getSpaceObjects() {return this.dataSpace.numObjects;}
    public int getPartitionBucketSize(){return dataSpace.partitionBucketSize;}
    
    /**
     * Calculate and return partition size from input size and ratio
     * NOTE: setSpaceObjects() must be called before calling this. Otherwise "divided by zero"
     * @param totalSize Input file size in bytes
     * @param ratio 
     */
    public int getPartitionBucketSize(long totalSize, int ratio){
    	if (dataSpace.partitionBucketSize == -1){
    	    int partSize = 0;
    	    double avgObjSize = totalSize / dataSpace.numObjects;
    	    dataSpace.partitionBucketSize = (int) Math.floor((blockSize * ratio) / avgObjSize);
    	}
    	return dataSpace.partitionBucketSize;
    }

    class Space implements Serializable
    {
	double dataMinX;
	double dataMinY;
	double dataMaxX;
	double dataMaxY;
	long numObjects = 0;
	int partitionBucketSize = -1;
	
	public String toString(){
	    return "dataMinX=" + dataMinX + "\ndataMinY=" + dataMinY + "\ndataMaxX=" + dataMaxX + "\ndataMaxY=" + dataMaxY + "\nnumobjects=" + numObjects + "\npartitionsize=" + partitionBucketSize;
	}
    }
}
