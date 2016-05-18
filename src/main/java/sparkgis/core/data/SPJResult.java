package sparkgis.core.data;
import java.io.Serializable;
import java.util.Iterator;

public class SPJResult implements Serializable
{
    private final int tileID;
    private final Iterable<String> results;
    
    public SPJResult(int id, Iterable<String>results){
	this.tileID = id;
	this.results = results;
    }
    
    public String toString(){
	String ret = tileID + "\t";
	Iterator it = results.iterator();
	while (it.hasNext()){
	    ret = ret + "\t" + it.next();
	}
	return ret;
    }
}
