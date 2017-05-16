package sparkgis.core.task;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Callable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import sparkgis.enums.HMType;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.SparkGISConfig;
import sparkgis.data.SpatialObject;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.pia.SparkSpatialJoinHM_Cogroup;

public class HeatMapTask extends Task implements Callable<String>{

    private final String hdfsPrefix = "hdfs://"+SparkGISConfig.hdfsNameNodeIP;
    
    private final List<String> algos;
    private final Predicate predicate;
    private final HMType type;
    private final String result_analysis_exe_id;

    private final Class dataType;
    
    private int algoCount;

    
    public HeatMapTask(SparkGISContext sgc,
		       String caseID,
		       List<String> algos,
		       Predicate predicate,
		       HMType hmType,
		       String result_analysis_exe_id,
		       Class dataType){
	super(sgc, caseID);
	
	this.algos = algos;
	this.predicate = predicate;
	this.type = hmType;
	algoCount = algos.size();
	this.result_analysis_exe_id = result_analysis_exe_id;
	this.dataType = dataType;
    }
    
    /**
     * Each HeatMapTask consists of 2 steps
     *   1. Generate configurations for algorithm pairs of input data (parallel)
     *   2. Generate heatmap from configurations
     */
    @Override
    public String call(){
	List<JavaRDD<TileStats>> results = new ArrayList<JavaRDD<TileStats>>();
	final List<Integer> pairs = generatePairs(algoCount);
	String caseID = "";
	
	if (dataType == SpatialObject.class){
	    List<DataConfig<SpatialObject>> configs = 
		sgc.prepareData(this.generateDataPaths());
	    for (int i=0; i<pairs.size(); i+=2){
		/* Step-2: Generate heatmap from configurations */
		if ((configs.get(i) != null) && (configs.get(i+1) != null)){
		    /* generate heatmap based from algo1 and algo2 data configurations */
		    results.add(generateHeatMap(configs.get(i), configs.get(i+1)));
		}
		else
		    System.out.println("Unexpected data configurations for caseID:"+super.data);
	    }
	    caseID = configs.get(0).getID();
	}
	else if (dataType == byte[].class){
	    /* Process Binary Data */
	    List<DataConfig<byte[]>> configs = 
		sgc.prepareBinaryData(this.generateDataPaths());
	    for (int i=0; i<pairs.size(); i+=2){
		/* Step-2: Generate heatmap from configurations */
		if ((configs.get(i) != null) && (configs.get(i+1) != null)){
		    /* generate heatmap based from algo1 and algo2 data configurations */
		    results.add(generateBinaryHeatMap(configs.get(i), configs.get(i+1)));
		}
		else
		    System.out.println("Unexpected data configurations for caseID:"+super.data);
	    }
	    caseID = configs.get(0).getID();
	}

	

	/* 
	 * heatmap stats generated for all algorithm pairs
	 * parameters to upload results to mongoDB or HDFS 
	 */
	final String resultsDir =
	    hdfsPrefix +
	    SparkGISConfig.hdfsHMResults +
	    sgc.getJobConf().getJobID() + "/";
	
	
	String orig_analysis_exe_id = algos.get(0);
	String title = "Spark-" + type.strValue + "-";
	for (String algo:algos)
	    title = title + algo + ":";
	// remove last ':' from tile
	title = title.substring(0, title.length()-1);
	String ret = "";

	for (JavaRDD<TileStats> result:results){
	    result.saveAsTextFile(resultsDir + super.data);
	}
	return resultsDir;
    }

    /**
     * Generate HDFS string paths for data
     * Similar function can be used to generate mongoDB strings for data
     */
    private List<String> generateDataPaths(){
	List<String> dataPaths = new ArrayList<String>();
	final String dataDir = hdfsPrefix + SparkGISConfig.hdfsAlgoData;

	for (int i=0; i<algoCount; ++i){
	    dataPaths.add(dataDir + "/" + algos.get(i) + "/" + super.data);
	}
	return dataPaths;
    }
    
    /**
     * Stage-2: Generate heatmap from data configurations
     */
    private JavaRDD<TileStats> generateHeatMap(DataConfig<SpatialObject> config1, DataConfig<SpatialObject> config2){
	SparkSpatialJoinHM_Cogroup<SpatialObject> heatmap1 =
	    new SparkSpatialJoinHM_Cogroup<SpatialObject>(sgc.getJobConf(),
					   config1,
					   config2,
					   predicate,
					   type
					   );
	return heatmap1.execute();
    }

    /**
     * Stage-2: Generate heatmap from data configurations
     */
    private JavaRDD<TileStats> generateBinaryHeatMap(DataConfig<byte[]> config1, DataConfig<byte[]> config2){
	SparkSpatialJoinHM_Cogroup<byte[]> heatmap1 =
	    new SparkSpatialJoinHM_Cogroup<byte[]>(sgc.getJobConf(),
					   config1,
					   config2,
					   predicate,
					   type
					   );
	return heatmap1.execute();
    }
}