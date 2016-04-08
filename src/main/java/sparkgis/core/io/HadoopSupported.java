package sparkgis.core.io;
/* Java imports */
import java.util.Map;
import java.util.UUID;
import java.util.LinkedHashMap;
import java.io.Serializable;
import java.lang.RuntimeException;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
/* Local imports */
import sparkgis.core.data.Polygon;
import sparkgis.core.data.HDFSPolygon;
import sparkgis.core.data.TileStats;
import sparkgis.SparkGIS;
import sparkgis.core.enums.Delimiter;

public abstract class HadoopSupported implements ISparkGISIO, Serializable{
    
    /**
     * @param Map params
     *        <path, value>      (path to input data)
     *        <delimiter, value> (delimiter value in line in input data)
     *        <geomIndex, value> (0 based geometry index in line in input data)
     * Assumes that input data contains one geometry information per line separated by delimiter
     * @return RDD of Polygons from input geometry data
     */
    public JavaRDD<Polygon> getPolygonsRDD(Map<String, Object> params){
	if (!params.containsKey("delimiter") || 
	    !params.containsKey("geomIndex") || 
	    !params.containsKey("path")
	    ){
	    throw new RuntimeException("[SparkGIS] Missing required parameter (delimiter OR geomIndex OR path)");
	}
	final String delim = (String)params.get("delimiter");
	final String path = (String)params.get("path");
	final int geomIndex = Integer.parseInt((String)params.get("geomIndex"));
	// in case input data contains only geometry generate id for each geometry
	if (geomIndex == 0){
	    // TODO ...
	    throw new RuntimeException("[SparkGIS] TODO: geomIndex is 0");
	}
	System.out.println("............... HDFS Path:" + path);

	return getDataAsText(path, delim).map(new Function<String, Polygon>(){
		public Polygon call(String s){
		    String[] fields = s.split(delim);
		    return new HDFSPolygon(fields[0], fields[geomIndex]);
		}
	    });
    }
    
    //public abstract JavaRDD<Polygon> getPolygonsRDD(String caseID, String algo);
    //public abstract JavaRDD<Polygon> getPolygonsRDD(String dataPath, Delimiter delim);
    /**
     * args[0]: caseID - hdfs/mongoDB
     * args[1]: analysis_execution_id - mongoDB only
     * args[2]: title - mongoDB only
     */
    public abstract String writeTileStats(JavaRDD<TileStats> result, String... args);

    public void writeRDD(JavaRDD data, String outPath){
	data.saveAsTextFile(outPath);
    }

    /**
     * Reads input data and return as it is
     * @param datapath path for data to read
     * @return JavaRDD String RDD of data
     */
    protected JavaRDD<String> getDataAsText(String datapath, final String delimiter){
	JavaRDD<String> rawTextData = 
	    SparkGIS.sc.textFile(datapath, SparkGIS.sc.defaultParallelism()).filter(new Function<String, Boolean>(){
		    public Boolean call(String s) {
			return (!s.isEmpty() && (s.split(delimiter).length >= 2));
		    }
		});
	return rawTextData;
    }
}
