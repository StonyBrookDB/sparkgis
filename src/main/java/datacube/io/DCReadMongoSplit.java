package datacube.io;
/* Java imports */
import java.util.Map;
import java.io.Serializable;
import java.util.concurrent.Callable;
/* Spark imports */
import sparkgis.core.data.Polygon;
import org.apache.spark.api.java.function.FlatMapFunction;
/* SparkGIS core includes */
import sparkgis.core.io.mongodb.ReadMongoSplit;
/* Local imports */
import datacube.data.DCObject;

public class DCReadMongoSplit 
//extends ReadMongoSplit
    implements Serializable//, FlatMapFunction<Long, DCObject>
{
    public DCReadMongoSplit(String host, int port, Map<String, String> params, int maxSplitSize){
	//super(host, port, params, maxSplitSize);
    }
        
    // /**
    //  * Called by Spark flatMap
    //  */
    // public Iterable<DCObject> call(Long splitStart){
    // 	return super.getDataSplit(splitStart, new DCObject());
    // }
}
