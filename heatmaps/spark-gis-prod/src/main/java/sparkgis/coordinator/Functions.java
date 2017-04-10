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
    
    
}
