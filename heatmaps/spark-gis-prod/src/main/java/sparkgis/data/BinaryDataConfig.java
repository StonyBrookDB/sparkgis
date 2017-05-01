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
public class BinaryDataConfig implements Serializable
{
    public final String caseID;
    /* 
     * geometry index in TAB separated string 
     * by default 1 (polygonID, polygonGeom)
     */
    private int geomid = 2;

    public JavaRDD<byte[]> originalData;
    
    /* Space stuff */
    private double dataMinX;
    private double dataMinY;
    private double dataMaxX;
    private double dataMaxY;
    private long numObjects = 0;
    
    public BinaryDataConfig(String caseID){
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

}
