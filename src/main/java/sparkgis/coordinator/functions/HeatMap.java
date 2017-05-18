package sparkgis.coordinator.functions;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Local imports */
import sparkgis.enums.HMType;
import sparkgis.enums.Predicate;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.core.task.HeatMapTask;
import sparkgis.core.task.BinaryHeatMapTask;

public class HeatMap{

    /**
     * Special direct function for heatmap generation for BMI data
     * @param spgc SparkGISContext 
     * @param algos List of algorithms to compare
     * @param caseIDs List of caseIDs to generate heatmap for in parallel
     * @param pred Predicate to use for spatial join
     * @param hmType Similarity coefficient to use for heatmap
     * @param result_analysis_exe_id Result id to show in caMicroscope 
     *                               (required only if writing result to MongoDB)  
     * @return Path to results heatmap directory
     */
    public static String execute(SparkGISContext spgc,
				 List<String> algos,
				 List<String> caseIDs,
				 Predicate pred,
				 HMType hmType
				 ){
	
	String resultsDirPath = null;
	/* create a thread pool for async jobs */
    	ExecutorService exeService =
	    Executors.newFixedThreadPool(spgc.getJobConf().getBatchFactor());
	
    	/* for a given algorithm pair create parallel heatmap generation tasks */
	List<HeatMapTask> tasks = new ArrayList<HeatMapTask>();
    	// List<BinaryHeatMapTask> tasks = new ArrayList<BinaryHeatMapTask>();
    	for (String caseID : caseIDs){
    	    HeatMapTask t =
    	    	new HeatMapTask(spgc,
    	    			caseID,
    	    			algos,
    	    			pred,
    	    			hmType);

	    // BinaryHeatMapTask t =
    	    // 	new BinaryHeatMapTask(spgc,
	    // 			      caseID,
	    // 			      algos,
	    // 			      pred,
	    // 			      hmType);
	    
    	    tasks.add(t);
    	}
    	/* wait for all jobs to complete */
    	try {
    	    List<Future<String>> results = exeService.invokeAll(tasks);
    	    for (Future res : results)
    		resultsDirPath = (String)res.get();
    	}catch(Exception e){e.printStackTrace();}
	
    	/* close thread pool */
    	exeService.shutdown();

	return resultsDirPath;
    }
    
}
