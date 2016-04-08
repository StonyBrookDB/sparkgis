package sparkgis.core.io.hdfs;
/* Java imports */
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.io.*;
import java.io.Serializable;
import java.net.URI;
//import java.lang.Runnable;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
/* Spark imports */
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
/* HDFS imports */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
//import org.apache.hadoop.fs.OutputStream;
import org.apache.hadoop.util.Progressable;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.SparkGISConfig;
import sparkgis.core.data.Tile;
//import sparkgis.io.ISparkGISIO;
import sparkgis.core.io.HadoopSupported;
import sparkgis.core.data.TileStats;
import sparkgis.core.data.DataConfig;
//import sparkgis.storagelayer.DataAccessLayer;
//import sparkgis.data.RawData;
import sparkgis.core.data.Polygon;
import sparkgis.core.enums.Delimiter;

public class HDFSDataAccess extends HadoopSupported implements Serializable
{
    private final String hdfsPrefix = "hdfs://"+SparkGISConfig.hdfsNameNodeIP;
    private final String coreSitePath = SparkGISConfig.hdfsCoreSitePath;
    private final String hdfsSitePath = SparkGISConfig.hdfsHdfsSitePath;
    // Incase all data has to be read from same directory
    //private String dataDir = SparkGISConfig.hdfsDataDir;
    //private String resultsDir = SparkGISConfig.hdfsOutDir;
    private String resultsDir = ""; // FIX THIS!!!

    /**
     * @param params
     *        <dataDir, value>
     *        <delimiter, value>
     * HDFS will create a path using the parameters e.g. if filterParams contains
     *        <algorithm, value>
     *        <caseID, value>
     * HDFS will try to read data from 
     *        SparkGISConfig.hdfsNameNodeIP/algorithm/caseID/
     * Note that the order in which keys were inserted to the Map matters here
     * therefore, there can be unreliable behavior if LinkedHashMap is not passed
     */
    public JavaRDD<Polygon> getPolygonsRDD(Map<String, Object> params){
	// if path variable doesnot exist, create one
	String path = params.containsKey("path") ? (String)params.get("path") : "";
	if (path.isEmpty()){
	    Iterator iterator = params.entrySet().iterator();
	    while(iterator.hasNext()) {
		Map.Entry entry = (Map.Entry)iterator.next();
		if (entry.getKey().equals("delimiter") || 
		    entry.getKey().equals("geomIndex")
		    )
		    continue;
		String val = (String)entry.getValue();
		if (val.startsWith("/"))
		    path = path + val;
		else
		    path = path + "/" + val;
	    }
	}
	
	// check if path contains namenode info as specified in configuration
	if (!path.startsWith(hdfsPrefix))
	    params.put("path", (hdfsPrefix + path));
	// call parent class method
	return super.getPolygonsRDD(params);
	
	
	// these are pia heatmap specific checks so should be moved to pia plugin
	// String delim = "";
	// String path = (String) params.get("dataDir");
	// Iterator iterator = params.keySet().iterator();
	// while(iterator.hasNext()) {
	//     Map.Entry entry = (Map.Entry)iterator.next();
	//     if (((String)entry.getKey()).equals("dataDir"))
	// 	continue;
	//     if (((String)entry.getKey()).equals("delimiter"))
	// 	delim = ((Delimiter)entry.getValue()).value;
	    
	//     path = path + "/" + entry.getValue();
	// }
	// return getDataAsText(path, delim).map(new Function<String, Polygon>(){
	// 	public Polygon call(String s){
	// 	    String[] fields = s.split("\t");
	// 	    return new HDFSPolygon(fields[0], fields[1]);
	// 	}
	//     });
    }
    
    // private JavaRDD<Polygon> getPolygonsRDD(String caseID, String algo){
    // 	return getDataAsText(dataDir + "/" + algo+"/"+caseID, "\t").map(new Function<String, Polygon>(){
    // 		public Polygon call(String s){
    // 		    String[] fields = s.split("\t");
    // 		    return new HDFSPolygon(fields[0], fields[1]);
    // 		}
    // 	    });
    // }

    public String writeTileStats(JavaRDD<TileStats> data, String... args){
	String caseID = args[0];
	data.saveAsTextFile(resultsDir + "/" + caseID);
	return (resultsDir + "/" + caseID);
    }
    
    /**
     * For simple SpatialJoin: modify this
     */
    // public JavaRDD<Polygon> getPolygonsRDD(final String dataPath, final Delimiter delim){
    // 	return getDataAsText(dataPath, delim.value).map(new Function<String, Polygon>(){
    // 		public Polygon call(String s){
    // 		    String[] fields = s.split(delim.value);
    // 		    // FIX THIS .... planet.dat.1 & europe.dat.2
    // 		    // ID, bla, bla, bla, Polygon
    // 		    return new HDFSPolygon(fields[0], fields[4]);
    // 		}
    // 	    });
    // }
    
    /**
     * Called from MongoToHDFS
     */
    public void writePolygons(List<Polygon> pList, String filePath){
	//String dataDir = hdfsPrefix + "fbaig/new-data/";
	try{
    	    Configuration config = new Configuration();
    	    config.addResource(new Path(coreSitePath));
    	    config.addResource(new Path(hdfsSitePath));
    	    FileSystem fs = FileSystem.get(config);
    	    URI pathURI = URI.create(filePath);
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
    	    URI pathURI = URI.create(filePath);
    	    Path path = new Path(pathURI);
    	    exists = fs.exists(path);
    	    fs.close();
    	} catch(Exception e) {e.printStackTrace();}
    	return exists;
    } 
}
