package sparkgis;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Spark imports */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
/* Local imports*/
import sparkgis.enums.HMType;
import sparkgis.io.ISparkGISIO;
import sparkgis.enums.Predicate;
import sparkgis.executionlayer.AlgoPair;
import sparkgis.io.mongodb.MongoDBDataAccess;
import sparkgis.executionlayer.task.HeatMapTask;

public class SparkGIS 
{
    public static JavaSparkContext sc;
    public static final char TAB = '\t';
    
    private final int threadCount = 8;
    
    public final ISparkGISIO inputSrc;
    public final ISparkGISIO outDest;
        
    public SparkGIS(ISparkGISIO inputSrc, ISparkGISIO out){
	this.inputSrc = inputSrc;
	this.outDest = out;
	// Initialize JavaSparkContext
	SparkConf conf = new SparkConf().setAppName("Spark-GIS");
	// set serializer
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	conf.set("textinputformat.record.delimiter", "\n");
	conf.set("spark.kryo.registrator", KryoClassRegistrator.class.getName());
    	sc = new JavaSparkContext(conf);
	SparkGIS.Debug("Default parallelism: " + sc.defaultParallelism());
    }
    
    public void heatMaps(String jobId,List<String> algos, List<String> caseIDs, Predicate pred, HMType hmType, int pSize, String result_analysis_exe_id){
	
	// create a thread pool for async jobs
    	ExecutorService exeService = Executors.newFixedThreadPool(threadCount);
	
	// for a given algorithm pair create parallel heatmap generation tasks
	List<HeatMapTask> tasks = new ArrayList<HeatMapTask>();
	for (String caseID : caseIDs){
	    HeatMapTask t = new HeatMapTask(jobId,inputSrc, caseID, algos, pred, hmType, outDest, result_analysis_exe_id);
	    // set optional parameters
	    t.setPartitionSize(pSize);
	    tasks.add(t);
	}
	// wait for all jobs to complete
	try {
	    List<Future<String>> results = exeService.invokeAll(tasks);
	    for (Future res : results)
		res.get();
	    //System.out.println(res.get());
	}catch(Exception e){e.printStackTrace();}
	
	//close thread pool
	exeService.shutdown();
	
    }
    
    public static < E > String createTSString(E... args){
    	String tss = "";
    	for (E arg : args){  
    	    if (tss == "")
    		tss = tss + arg;
    	    else
    		tss = tss + TAB + arg;
    	}
    	//tss = tss + "\n";
    	return tss;
    }

    public static < E > void Debug(E... vals){
	// System.out.println("");
	// System.out.print("[DEBUG] ");
	// for (E val:vals){
	//     System.out.print(val + " ");
	// }
	// System.out.println("");
    }
}