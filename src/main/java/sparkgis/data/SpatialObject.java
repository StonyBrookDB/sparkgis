package sparkgis.data;

import java.io.Serializable;

public class SpatialObject implements Serializable
{
    protected final String id;
    protected final String spatialDataString;

    private long tileID;
    
    public SpatialObject(String spatialDataString){
	this.id = sparkgis.coordinator.SparkGISJobConf.DUMMY_ID;
	this.spatialDataString = spatialDataString;
	
    }
    public SpatialObject(String id, String spatialDataString){
	this.id = id;
	this.spatialDataString = spatialDataString;
    }

    public void setTileID(long tileID){this.tileID = tileID;}
    public long getTileID(){return this.tileID;}
    
    public String getId(){return this.id;}
    public String getSpatialData(){return this.spatialDataString;}

    public String toString(){
	if (id == "") return getSpatialData();
	return id + "\t" + getSpatialData();
    }
}
