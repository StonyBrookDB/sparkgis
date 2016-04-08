package sparkgis.core.io.local;
/* Java imports */
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.io.*;
import java.io.Serializable;
import java.net.URI;
import java.lang.RuntimeException;
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
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.core.data.Tile;
import sparkgis.core.io.HadoopSupported;
import sparkgis.core.data.TileStats;
import sparkgis.core.data.DataConfig;
import sparkgis.core.data.Polygon;
import sparkgis.core.data.HDFSPolygon;
import sparkgis.core.enums.Delimiter;

public class LocalDataAccess extends HadoopSupported implements Serializable
{
    private String resultsDir = "file:///data01/shared/fbaig/results/";
    private final String localPrefix = "file://"; 
    
    public LocalDataAccess(){}

    public JavaRDD<Polygon> getPolygonsRDD(Map<String, Object> params){
	// if path variable doesnot exist, create one
	String path = params.containsKey("path") ? (String)params.get("path") : "";
	if (path.isEmpty()){
	    Iterator iterator = params.keySet().iterator();
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
	if (!path.startsWith(localPrefix))
	    params.put("path", (localPrefix + "/" + path));
	// call parent class method
	return super.getPolygonsRDD(params);	
    }

    
    // public JavaRDD<Polygon> getPolygonsRDD(String caseID, String algo){
    // 	return getDataAsText(algo+"/"+caseID, "\t").map(new Function<String, Polygon>(){
    // 		public Polygon call(String s){
    // 		    String[] fields = s.split("\t");
    // 		    return new HDFSPolygon(fields[0], fields[1]);
    // 		}
    // 	    });
    // }
    
    /**
     * For simple SpatialJoin: modify this
     */
    // public JavaRDD<Polygon> getPolygonsRDD(String dataPath, final Delimiter delim){
    // 	//final String delimiter = "\\|";
    // 	SparkGIS.Debug("LocalDataAccess.getPolygonsRDD(): " + dataPath);
    // 	return getDataAsText(dataPath, delim.value).map(new Function<String, Polygon>(){
    // 		public Polygon call(String s){
    // 		    String[] fields = s.split(delim.value);
    // 		    // FIX THIS .... planet.dat.1 & europe.dat.2
    // 		    // ID, bla, bla, bla, Polygon
    // 		    return new HDFSPolygon(fields[0], fields[4]);
    // 		}
    // 	    });
    // }

    public String writeTileStats(JavaRDD<TileStats> data, String... args){
	String caseID = args[0];
	data.saveAsTextFile(resultsDir + "/" + caseID);
	return (resultsDir + "/" + caseID);
    }
}
