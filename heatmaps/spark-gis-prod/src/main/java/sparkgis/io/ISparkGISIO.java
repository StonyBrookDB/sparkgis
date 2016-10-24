package sparkgis.io;
/* Java imports */
import java.util.List;
import java.io.Serializable;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import sparkgis.data.Polygon;
import sparkgis.data.TileStats;

public interface ISparkGISIO
{
    public JavaRDD<Polygon> getPolygonsRDD(String caseID, String algo);
    /**
     * args[0]: caseID - hdfs/mongoDB
     * args[1]: analysis_execution_id - mongoDB only
     * args[2]: title - mongoDB only
     */
    public String writeTileStats(JavaRDD<TileStats> result, String... args);
}
