package sparkgis.executionlayer.task;
// /* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import sparkgis.enums.HMType;
import sparkgis.data.Polygon;
import sparkgis.data.TileStats;
import sparkgis.io.ISparkGISIO;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.executionlayer.AlgoPair;
import sparkgis.executionlayer.SparkPrepareData;
import sparkgis.executionlayer.SparkSpatialJoinHM;

public class HeatMapTask extends Task implements Callable<String>{
    //private final AlgoPair algos;
    private final List<String> algos;
    private final Predicate predicate;
    private final HMType type;
    private final String result_analysis_exe_id;

     private String   jobId;


    private int partitionSize = 512; // Default
    
    // To generate data configurations in parallel
    private int algoCount = 2;
    private final ExecutorService exeService = Executors.newFixedThreadPool(algoCount);
    
    public HeatMapTask(String jobId,ISparkGISIO inputSrc, String caseID, List<String> algos, Predicate predicate, HMType hmType, ISparkGISIO outDest, String result_analysis_exe_id){
	super(inputSrc, outDest, caseID);
 
 this.jobId = jobId;
	this.algos = algos;
	this.predicate = predicate;
	this.type = hmType;
	this.partitionSize = partitionSize;
	algoCount = algos.size();
	this.result_analysis_exe_id = result_analysis_exe_id;
    }

    
    public void setPartitionSize(int pSize){this.partitionSize = pSize;}
    
    
    /**
     * Each HeatMapTask consists of 2 steps
     *   1. Generate configurations for algorithm pairs of input data (parallel)
     *   2. Generate heatmap from configurations
     */
    @Override
    public String call(){
	List<JavaRDD<TileStats>> results = new ArrayList<JavaRDD<TileStats>>();

	sparkgis.stats.Profile.log.put("HeatMapTask-"+super.data, System.nanoTime());
	
	DataConfig[] configs = new DataConfig[algoCount];
	List<Future<DataConfig>> futures = new ArrayList<Future<DataConfig>>();
	for (int i=0; i<algoCount; ++i){
	    futures.add(exeService.submit(new AsyncPrepareData(super.data, algos.get(i))));
	}
	try{
	    for (int i=0; i<algoCount; ++i)
		configs[i] = futures.get(i).get();
	}catch(Exception e){e.printStackTrace();}
	// close thread pool
	exeService.shutdown();

	sparkgis.stats.Profile.log.put("[BEGIN] Spatial Processing-"+super.data, System.nanoTime());
	//final List<AlgoPair> algoPairs = generatePairs(algos);
	final List<Integer> pairs = generatePairs();
	for (int i=0; i<pairs.size(); i+=2){
	    /* Step-2: Generate heatmap from configurations */
	    if ((configs[i] != null) && (configs[i+1] != null)){
		// generate heatmap based from algo1 and algo2 data configurations
		results.add(generateHeatMap(configs[i], configs[i+1]));
		sparkgis.stats.Profile.printProgress();
	    }
	    else System.out.println("Unexpected data configurations for caseID:"+super.data);
	}
	sparkgis.stats.Profile.log.put("[END] Spatial Processing & \n[BEGIN] Writing results-"+super.data, System.nanoTime());
	// heatmap stats generated for all algorithm pairs
	// parameters to upload results to mongoDB
	String caseID = configs[0].caseID;
	String orig_analysis_exe_id = algos.get(0);
	String title = "Spark-" + type.strValue + "-";
	for (String algo:algos)
	    title = title + algo + ":";
	// remove last ':' from tile
	title = title.substring(0, title.length()-1);
	String ret = "";
 
	System.out.println("shere - baig");
	for (JavaRDD<TileStats> result:results){
	    ret = outDest.writeTileStats(result, caseID, orig_analysis_exe_id, title, result_analysis_exe_id,jobId);
	    System.out.println("completed");
	}
	sparkgis.stats.Profile.log.put("Done-"+super.data, System.nanoTime());
	return ret;
    }

    /**
     * Stage-1: Inner class to get data from Input source and generate data configuration
     */
    public class AsyncPrepareData implements Callable<DataConfig>{
        private final String caseID;
	private final String algo;	
    	public AsyncPrepareData(String caseID, String algo){
	    this.caseID = caseID;
	    this.algo = algo;    	    
    	}
	
	@Override
	public DataConfig call(){
	    //long start = System.nanoTime();
	    sparkgis.stats.Profile.log.put("[BEGIN] IO-"+caseID+"-"+algo, System.nanoTime());
	    // get data from input source and keep in memory
	    JavaRDD<Polygon> polygonsRDD = inputSrc.getPolygonsRDD(caseID, algo).cache();
	    long objCount = polygonsRDD.count();	
	    sparkgis.stats.Profile.log.put("[END] IO-"+caseID+"-"+algo, System.nanoTime());
	    //Profile.log("[Obj]" + super.data + "-" + algo, objCount);
	    //start = System.nanoTime();
	    if (objCount != 0){
		sparkgis.stats.Profile.log.put("[BEGIN] PrepareData-"+caseID+"-"+algo, System.nanoTime());
		// Invoke spark job: Prepare Data
		SparkPrepareData job = new SparkPrepareData(caseID);
		DataConfig ret = job.execute(polygonsRDD);
		sparkgis.stats.Profile.log.put("[END] PrepareData-"+caseID+"-"+algo, System.nanoTime());
		return ret;
	    }
	    return null;
	}
    }
    
    /**
     * Stage-2: Generate heatmap from data configurations
     */
    private JavaRDD<TileStats> generateHeatMap(DataConfig config1, DataConfig config2){
	//long start = System.nanoTime();
	SparkSpatialJoinHM heatmap1 = new SparkSpatialJoinHM(config1, config2, predicate, type, partitionSize);
	//JavaRDD<TileStats> result = heatmap1.execute();
	return heatmap1.execute();
	//result.first(); // JUST FOR EXPERIMENTAL RESULTS. COMMENT IF OUTPUTTING RESULTS
	// Profile.log("[EXE-HM]" + config1.caseID, System.nanoTime() - start);
    }

    private List<Integer> generatePairs(){
	ArrayList<Integer> ret = new ArrayList<Integer>();
	for (int i=0; i<algoCount; ++i){
	    for (int j=(i+1); j<algoCount; ++j){
		ret.add(i);
		ret.add(j);
	    }
	}
	return ret;
    }
    
    private List<AlgoPair> generatePairs(List<String> algos){
	ArrayList<AlgoPair> ret = new ArrayList<AlgoPair>();
	for (int i=0; i<algos.size(); ++i){
	    for (int j=(i+1); j<algos.size(); ++j)
		ret.add(new AlgoPair(algos.get(i), algos.get(j)));
	}
	return ret;
    }
}
