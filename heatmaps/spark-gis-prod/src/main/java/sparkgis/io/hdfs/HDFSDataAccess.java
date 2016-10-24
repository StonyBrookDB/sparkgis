package sparkgis.io.hdfs;
/* Java imports */
import java.io.*;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
/* Spark imports */
import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
/* HDFS imports */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.data.Tile;
import sparkgis.data.Polygon;
import sparkgis.SparkGISConfig;
import sparkgis.io.ISparkGISIO;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
import sparkgis.data.HDFSPolygon;

public class HDFSDataAccess implements ISparkGISIO, Serializable
{
    private static final char TAB = '\t';

    private final String hdfsPrefix = "hdfs://"+SparkGISConfig.hdfsNameNodeIP; //130.245.124.11/user/"; // nfs001 - gigabit
    private String coreSitePath = SparkGISConfig.hdfsCoreSitePath;
    private String hdfsSitePath = SparkGISConfig.hdfsHdfsSitePath;
    private String dataDir = hdfsPrefix + SparkGISConfig.hdfsAlgoData;
    private String resultsDir = hdfsPrefix + SparkGISConfig.hdfsHMResults;

    public HDFSDataAccess(){}
    /**
     * @param coreSitePath Hadoop configuration path for core-site.xml
     * @param hdfsSitePath Hadoop configuration path for hdfs-site.xml
     * @param dataDir Prefix hdfs path for data directory (Might note be needed)
     * @param outDir Prefix hdfs path to write results to
     */
    public HDFSDataAccess(String coreSitePath, String hdfsSitePath, String dataDir, String outDir){
	this.coreSitePath = coreSitePath;
	this.hdfsSitePath = hdfsSitePath;
	this.dataDir = dataDir;
	this.resultsDir = outDir;
    }

    /**
     * Append jobID to results directory
     * @param jobId
     */
    public void appendResultsDir(String jobId){this.resultsDir = this.resultsDir + jobId + "/";}
    public String getResultsDir(){return this.resultsDir;}
    
    /**
     * Reads input data and return as it is
     * @param datapath HDFS path for data to read
     * @param persist Boolean value to keep RDD in memory or not
     * @return JavaRDD String RDD of data
     */
    private JavaRDD<String> getDataAsText(String datapath){
	JavaRDD<String> rawTextData = 
	    SparkGIS.sc.textFile(dataDir + "/" + datapath, SparkGIS.sc.defaultParallelism()).filter(new Function<String, Boolean>(){
		    public Boolean call(String s) {
			return (!s.isEmpty() && (s.split("\t").length >= 2));
		    }
		});
	return rawTextData;
    }
    
    public JavaRDD<Polygon> getPolygonsRDD(String caseID, String algo){
	return getDataAsText(algo+"/"+caseID).map(new Function<String, Polygon>(){
		public Polygon call(String s){
		    String[] fields = s.split("\t");
		    return new HDFSPolygon(fields[0], fields[1]);
		}
	    });
    }

    public String writeTileStats(JavaRDD<TileStats> data, String... args){
	String caseID = args[0];
	data.saveAsTextFile(resultsDir + "/" + caseID);
	return (resultsDir + "/" + caseID);
    }
    
    /**
     * Called from MongoToHDFS
     */
    public void writePolygons(List<Polygon> pList, String filePath){
	try{
    	    Configuration config = new Configuration();
    	    config.addResource(new Path(coreSitePath));
    	    config.addResource(new Path(hdfsSitePath));
    	    FileSystem fs = FileSystem.get(config);
    	    URI pathURI = URI.create(dataDir + "/" + filePath);
    	    Path path = new Path(pathURI);
    	    FSDataOutputStream fos = fs.create(path);
    	    BufferedWriter pWriter = new BufferedWriter( new OutputStreamWriter( fos, "UTF-8" ) );
	    for (Polygon p : pList)
		pWriter.write(p.toString() + "\n");
	    pWriter.close();
	    fs.close();
	}catch(Exception e){e.printStackTrace();}
    }
    
    public boolean fileExists(String filePath){
    	boolean exists = false;
    	try{
    	    Configuration config = new Configuration();
    	    config.addResource(new Path(coreSitePath));
    	    config.addResource(new Path(hdfsSitePath));
    	    FileSystem fs = FileSystem.get(config);
    	    URI pathURI = URI.create(dataDir + "/" + filePath);
    	    Path path = new Path(pathURI);
    	    exists = fs.exists(path);
    	    fs.close();
    	} catch(Exception e) {e.printStackTrace();}
    	return exists;
    }  
}
