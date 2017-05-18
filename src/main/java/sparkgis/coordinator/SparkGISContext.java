package sparkgis.coordinator;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
/* Local imports */
import sparkgis.data.DataConfig;
import sparkgis.data.SpatialObject;
import sparkgis.data.BinaryDataConfig;


public class SparkGISContext {

    public static final String TAB = "\t";
    public static JavaSparkContext sparkContext;

    private final SparkGISJobConf jobConf;

    /**
     * SparkGISContext constructor with default SparkGISJobConf
     */
    public SparkGISContext(){
	this(null, null);
    }

    /**
     * SparkGISContext constructor with cutom configurations
     * @param conf SparkConf object
     * @param jobConf Custom SparkGIS job configuration
     */
    public SparkGISContext(SparkConf conf, SparkGISJobConf jobConf){
	this.sparkContext = (conf == null)? new JavaSparkContext() : new JavaSparkContext(conf);
	this.jobConf = (jobConf == null) ? new SparkGISJobConf() : jobConf;
    }

    /**
     * @return Current SparkGIS job configuration
     */
    public SparkGISJobConf getJobConf(){return this.jobConf;}

    /**
     * Prepare JavaRDD's with instances of SpatialObjects
     * @param dataPaths HDFS paths to input spatial data
     * @return List of dataConfig objects with raw spatialObjects
     * and required preprocessed spatial data
     */
    public List<DataConfig> prepareData(List<String> dataPaths){
	return (new PrepareData(jobConf.getDelimiter(),
				jobConf.getSpatialObjectIndex())).prepareData(dataPaths);
    }

    /**
     * Prepare JavaRDD's with binary spatial data
     * @param dataPaths HDFS paths to input spatial data
     * @return List of dataConfig objects with binary spatial obejcts
     * and required preprocessed spatial data
     */
    public List<BinaryDataConfig> prepareBinaryData(List<String> dataPaths){
	return (new PrepareBinaryData(jobConf.getDelimiter(),
				jobConf.getSpatialObjectIndex())).prepareBinaryData(dataPaths);
    }

    /**
     * Stop SparkGISContext as well as Apache Spark Context
     */
    public void stop(){
	this.sparkContext.stop();
    }

    /**
     * A simple utility function to create TAB separated string
     * from a list of strings
     * @return A TAB separated string
     */
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
