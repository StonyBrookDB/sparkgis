package datacube.data;

/* Java imports */
import java.io.Serializable;

public class DoubleProperty<T> extends Property implements Serializable{
    
    private Double min;
    private Double max;
    
    public DoubleProperty(PropertyName name, T value){
	super(name, value);
    }
    
    public DoubleProperty(PropertyName name, Double value){
	super(name, value);
    }
    
    public Double getValue(){return (Double) super.value;}
}