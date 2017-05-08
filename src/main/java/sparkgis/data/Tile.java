package sparkgis.data;

import java.io.Serializable;

public class Tile implements Serializable
{
    public long tileID;
    public double minX;
    public double minY;
    public double maxX;
    public double maxY;
    // Just a hack To avoid an extra call to count OR map to calculate total tiles
    public long count = 1;
    
    @Override
    @SuppressWarnings("unchecked")
    public String toString()
    {
	return sparkgis.coordinator.SparkGISContext.
	    createTSString(tileID, minX, minY, maxX, maxY);
    }
}
