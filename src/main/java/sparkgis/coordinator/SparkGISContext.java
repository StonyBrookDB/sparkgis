package sparkgis.coordinator;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Spark imports */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
/* JTS imports */
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.ByteOrderValues;
/* Local imports */
import sparkgis.data.DataConfig;
import sparkgis.data.BinaryDataConfig;
import sparkgis.data.SpatialObject;
import sparkgis.core.SparkGISPrepareData;
import sparkgis.core.SparkGISPrepareBinaryData;

public class SparkGISContext {

    public static final String TAB = "\t";
    public static JavaSparkContext sparkContext;

    private final SparkGISJobConf jobConf;
    
    /**
     * SparkGISContext constructor with default SparkGISJobConf
     */
    public SparkGISContext(){
	this.sparkContext = new JavaSparkContext();
	this.jobConf = new SparkGISJobConf();
    }

    /**
     * SparkGISContext constructor with cutom configurations
     * @param conf SparkConf object
     * @param jobConf Custom SparkGIS job configuration
     */
    public SparkGISContext(SparkConf conf, SparkGISJobConf jobConf){
	this.sparkContext = new JavaSparkContext(conf);
	this.jobConf = jobConf;
    }

    /**
     * @return Current SparkGIS job configuration
     */
    public SparkGISJobConf getJobConf(){return this.jobConf;}

    public List<DataConfig> prepareData(List<String> dataPaths){
	return (new PrepareData(jobConf.getDelimiter(),
				jobConf.getSpatialObjectIndex())).prepareData(dataPaths);
    }

    public List<BinaryDataConfig> prepareBinaryData(List<String> dataPaths){
	return (new PrepareBinaryData(jobConf.getDelimiter(),
				jobConf.getSpatialObjectIndex())).prepareBinaryData(dataPaths);
    }
    
    public void stop(){
	this.sparkContext.stop();
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
