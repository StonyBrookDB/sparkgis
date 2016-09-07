package sparkgis.executionlayer.task;

/* Java imports */
import java.util.List;
/* Local imports*/
import sparkgis.io.ISparkGISIO;

public class Task{
    protected final ISparkGISIO inputSrc;
    protected final ISparkGISIO outDest;
    protected final String data;
    
    public Task(ISparkGISIO inputSrc, ISparkGISIO outDest, String data){
	this.inputSrc = inputSrc;
	this.outDest = outDest;
	this.data = data;
    }
}
