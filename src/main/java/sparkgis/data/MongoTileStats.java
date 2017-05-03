package sparkgis.data;
import java.io.Serializable;

public class MongoTileStats extends TileStats implements Serializable, Comparable<TileStats>
{
    public String algorithm;
    public String title;
    
    public MongoTileStats(){
	
    }
}
