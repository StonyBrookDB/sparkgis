package datacube.data;
/* Java imports*/
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

/*
 * Uncomment all lines containing T in case of any issue
 * T was working
 */

/**
 * Each object read from MongoDB has a number of assiciated properties
 * This class contains all properties that have to be read in memory for each object
 * These properties eventually gets translated to datacube dimensions
 */
//public class Property<T> implements Serializable{
public class Property implements Serializable{

    private final PropertyName name;
    //protected final T value;
    protected final Object value;

    //public Property(PropertyName name, T value){
    public Property(PropertyName name, Object value){
	this.name = name;
	this.value = value;
    }
    
    public boolean equals(Property obj){
	return obj.getNameStr().equals(this.getNameStr());
    }
    
    //public T getValue() {return value;}
    public Object getValue() {return value;}
    // public Double getDoubleValue(){
    // 	if (value instanceof Double)
    // 	    return (Double) value;
    // 	throw new RuntimeException ("[SparkGIS] Value is not of type Double");
    // }
    // public String getStringValue(){
    // 	if (value instanceof String)
    // 	    return (String) value;
    // 	throw new RuntimeException ("[SparkGIS] Value is not of type String");
    // }
    public String getNameStr() {return name.value;}
    public PropertyName getName() {return name;}

    public static boolean checkValidPropertyName(String NameStr){
	List<PropertyName> allPropertyNames = Arrays.asList(PropertyName.values());
	
	for(PropertyName pName:allPropertyNames){
	    if (NameStr.equalsIgnoreCase(pName.value))
		return true;//pName.value;
	}
	return false;
    }
}
