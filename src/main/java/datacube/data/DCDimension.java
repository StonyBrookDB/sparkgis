package datacube.data;
/* Java imports */
import java.io.Serializable;
/* Local imports */
import datacube.data.PropertyName;

public class DCDimension implements Serializable{
    private final PropertyName name;
    private final int bucketCount; // number of buckets
    private Double resolution = null;
    private Double min;
    private Double max;
    
    public DCDimension(PropertyName name, int bucketCount){
	this.name = name;
	    this.bucketCount = bucketCount;
    }
    
    public void setMin(double min){this.min = min;}
    public void setMax(double max){this.max = max;}
    
    public PropertyName getName(){return name;}
    public String getNameStr(){return name.value;}
    public int getBucketCount(){return bucketCount;}
    public Double getResolution(){
	if (resolution == null)
	    resolution = new Double((max-min)/bucketCount);
	return resolution;
    }
    public Double getMin(){return min;}
    public Double getMax(){return max;}
}
