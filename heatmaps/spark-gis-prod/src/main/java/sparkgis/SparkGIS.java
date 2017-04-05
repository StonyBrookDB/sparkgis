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
      
    public SparkGIS(ISparkGISIO inputSrc, ISparkGISIO out){
	// this.inputSrc = inputSrc;
	// this.outDest = out;
	/* Initialize JavaSparkContext */
	SparkConf conf = new SparkConf().setAppName("Spark-GIS");
	/* set serializer */
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	conf.set("textinputformat.record.delimiter", "\n");
	conf.set("spark.kryo.registrator", KryoClassRegistrator.class.getName());
    	sc = new JavaSparkContext(conf);
    }
    
    /**
     * Create one time index for given data before spatial operation
     * (1) Prepare data
     * (2) 
     */
    public void indexedPartition(){
	
    }
    
    public static < E > String createTSString(E... args){
    	String tss = "";
    	for (E arg : args){  
    	    if (tss == "")
    		tss = tss + arg;
    	    else
    		tss = tss + TAB + arg;
    	}
    	return tss;
    }
}
