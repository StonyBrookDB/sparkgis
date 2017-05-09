package sparkgis.data;
/* Java imports */
import java.io.Serializable;

public class Space implements Serializable{
    
    
    /* Space stuff */
    private double dataMinX;
    private double dataMinY;
    private double dataMaxX;
    private double dataMaxY;
    private long numObjects = 0;

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

    public void setMinX(double minX){this.dataMinX = minX;}
    public void setMinY(double minY){this.dataMinY = minY;}
    public void setMaxX(double maxX){this.dataMaxX = maxX;}
    public void setMaxY(double maxY){this.dataMaxY = maxY;}
    public void setSpaceObjects(long count){this.numObjects = count;}
    
}
