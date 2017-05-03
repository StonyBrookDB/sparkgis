package sparkgis.debug;
/* Java imports */
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import sparkgis.data.DataConfig;

public class DebugToDisk
{
    public static void writeMBBStats(DataConfig config, String path){
	try {
    	    PrintWriter out = 
    		new PrintWriter(new BufferedWriter(new FileWriter(path, true)));
	    out.println(config.toString());
	    out.close();
	}catch(Exception e){e.printStackTrace();}
    }

    public static void writeRDD(JavaRDD data, String path){
    	data.saveAsTextFile(path);
    }
}
