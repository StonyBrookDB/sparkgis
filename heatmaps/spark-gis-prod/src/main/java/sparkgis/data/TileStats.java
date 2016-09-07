package sparkgis.data;
import java.io.Serializable;
import sparkgis.enums.HMType;

public class TileStats implements Serializable, Comparable<TileStats>
{
    public Tile tile;
    public double statistics = -1;
    public String type;
    
    @Override
    public int compareTo(TileStats t){
    	// descending order
    	Double d1 = new Double(this.statistics);
    	Double d2 = new Double(t.statistics);
    	return d2.compareTo(d1);
    }
    
    public String toString(){
	return statistics + "\t" + tile.toString();
    }
}
