package datacube.task;
/* Java imports */
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import datacube.data.DCObject;
import datacube.io.DCMongoDBDataAccess;

public class DataLoadTask //implements Callable<JavaRDD<DCObject>>
{
    // private final DCMongoDBDataAccess inputSrc;

    // private final Long batchStart;
    // //private final ExecutorService exeService;

    // private final Map<String, Object> filterParams;

    // public DataLoadTask(
    // 			final Long batchStart, 
    // 			final DCMongoDBDataAccess inputSrc,
    // 			final Map<String, Object> filterParams
    // 			){
    // 	this.inputSrc = inputSrc;
    // 	this.batchStart = batchStart;
    // 	//exeService = Executors.newFixedThreadPool(loadBatchSize);

    // 	this.filterParams = filterParams;
    // }

    // public JavaRDD<DCObject> call(){
	
    // 	return inputSrc.getDataRDD(filterParams, batchStart, (long)10000);
	
    // 	//return "";
    // }

}
