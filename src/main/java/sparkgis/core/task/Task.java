package sparkgis.core.task;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
/* Local imports*/
import sparkgis.coordinator.SparkGISContext;

public class Task{
    protected final String data;
    protected final SparkGISContext sgc;
    
    public Task(SparkGISContext sgc, String data){
	this.sgc = sgc;
	this.data = data;
    }

    protected List<Integer> generatePairs(int count){
    	ArrayList<Integer> ret = new ArrayList<Integer>();
    	for (int i=0; i<count; ++i){
    	    for (int j=(i+1); j<count; ++j){
    		ret.add(i);
    		ret.add(j);
    	    }
    	}
    	return ret;
    }
}
