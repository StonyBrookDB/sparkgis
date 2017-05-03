package sparkgis.executionlayer.task;
/* Java imports */
import java.util.concurrent.Callable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import sparkgis.data.Polygon;
import sparkgis.data.DataConfig;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.executionlayer.SparkPrepareData;

/**
 * Stage-1: Inner class to get data from Input source and generate data configuration
 */
public class AsyncPrepareData implements Callable<DataConfig>{
    private final String caseID;
    private final String algo;
    private final SparkGISContext sgc;
    
    public AsyncPrepareData(SparkGISContext sgc, String caseID, String algo){
	this.sgc = sgc;
	this.caseID = caseID;
	this.algo = algo;    	    
    }
	
    @Override
    public DataConfig call(){
	/* get data from input source and keep in memory */
	// JavaRDD<Polygon> polygonsRDD = sgc.inputSrc.getPolygonsRDD(caseID, algo).cache();
	// long objCount = polygonsRDD.count();
	// if (objCount != 0){
	//     /* Invoke spark job: Prepare Data */
	//     SparkPrepareData job = new SparkPrepareData(caseID);
	//     DataConfig ret = job.execute(polygonsRDD);
	//     return ret;
	// }
	return null;
    }
}
