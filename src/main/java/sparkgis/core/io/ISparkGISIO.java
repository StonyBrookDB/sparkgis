package sparkgis.core.io;
/* Java imports */
import java.util.Map;
import java.util.List;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import sparkgis.core.data.Polygon;
import sparkgis.core.data.TileStats;
import sparkgis.core.enums.Delimiter;

public interface ISparkGISIO //implements Serializable
{

    //public JavaRDD getDCObjectRDD(Map<String, String> filterParams);
    public JavaRDD getPolygonsRDD(Map<String, Object> filterParams);
    
    /**
     * args[0]: caseID - hdfs/mongoDB
     * args[1]: analysis_execution_id - mongoDB only
     * args[2]: title - mongoDB only
     */
    public String writeTileStats(JavaRDD<TileStats> result, String... args);
    
    public void writeRDD(JavaRDD data, String outPath);
    


    //public void writeTileStats(List<TileStats> result, String caseID);
    //public JavaRDD<Polygon> getPolygonsRDD(String caseID, String algo);
    //public JavaRDD<Polygon> getPolygonsRDD(String dataPath, Delimiter delim);
}
