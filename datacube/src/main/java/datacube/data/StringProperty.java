package datacube.data;

public class StringProperty extends Property{
    
    public StringProperty(PropertyName name, String value){
	super(name, value);
    }
    
    public String getValue(){return (String) super.value;}
}