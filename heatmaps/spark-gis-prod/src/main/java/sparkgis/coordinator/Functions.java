package sparkgis.coordinator;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Local imports*/
import sparkgis.enums.HMType;
import sparkgis.io.ISparkGISIO;
import sparkgis.enums.Predicate;
import sparkgis.executionlayer.AlgoPair;
import sparkgis.io.mongodb.MongoDBDataAccess;
import sparkgis.executionlayer.task.HeatMapTask;

public class Functions{

    /**
     * Spatial Join on given datasets
     * @param datasetPath1 HDFS path for spatial dataset1
     * @param datasetPath2 HDFS path for spatial dataset1
     * @param pred Enum Predicate for spatial join operations
     */
    public static void spatialJoin(String datasetPath1,
				   String datasetPath2,
				   Predicate pred){
	
    }
    /**
     * Spatial Range on given datasets
     * @param datasetPath1 HDFS path for spatial dataset1
     * @param datasetPath2 HDFS path for spatial dataset1
     */
    public static void spatialRange(String datasetPath1,
				    String datasetPath2){
	
    }
    
    /**
     * Special direct function for heatmap generation for BMI data
     * @param jobID
     * @param algos List of algorithms to compare
     * @param caseIDs List of caseIDs to generate heatmap for in parallel
     * @param pred Predicate to use for spatial join
     * @param hmType Similarity coefficient to use for heatmap
     * @param pSize Partition size
     * @param result_analysis_exe_id Result id to show in caMicroscope 
     *                               (required only if writing result to MongoDB)  
     */
    public static void heatMaps(SparkGISContext spgc,
				List<String> algos,
				List<String> caseIDs,
				Predicate pred,
				HMType hmType,
				int pSize,
				String result_analysis_exe_id){
	
    	// /* create a thread pool for async jobs */
    	// ExecutorService exeService = Executors.newFixedThreadPool(spgc.getBatchFactor());
	
    	// /* for a given algorithm pair create parallel heatmap generation tasks */
    	// List<HeatMapTask> tasks = new ArrayList<HeatMapTask>();
    	// for (String caseID : caseIDs){
    	//     HeatMapTask t =
    	// 	new HeatMapTask(spgc.jobID,
    	// 			spgc.inputSrc,
    	// 			caseID,
    	// 			algos,
    	// 			pred,
    	// 			hmType,
    	// 			spgc.outDest,
    	// 			result_analysis_exe_id);
    	//     /* set optional parameters */
    	//     t.setPartitionSize(pSize);
    	//     tasks.add(t);
    	// }
    	// /* wait for all jobs to complete */
    	// try {
    	//     List<Future<String>> results = exeService.invokeAll(tasks);
    	//     for (Future res : results)
    	// 	res.get();
    	// }catch(Exception e){e.printStackTrace();}
	
    	// /* close thread pool */
    	// exeService.shutdown();
    }
}
