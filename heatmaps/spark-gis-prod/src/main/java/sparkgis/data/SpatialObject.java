package sparkgis.data;

import java.io.Serializable;

public class SpatialObject implements Serializable
{
    protected final String id;
    protected final String spatialDataString;
    
    public SpatialObject(String spatialDataString){
	this.id = sparkgis.coordinator.SparkGISJobConf.DUMMY_ID;
	this.spatialDataString = spatialDataString;
	
    }
    public SpatialObject(String id, String spatialDataString){
	this.id = id;
	this.spatialDataString = spatialDataString;
    }

    public String getId(){return this.id;}
    public String getSpatialData(){return this.spatialDataString;}

    public String toString(){
	if (id == "") return getSpatialData();
	return id + "\t" + getSpatialData();
    }
}
