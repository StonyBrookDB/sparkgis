package sparkgis;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;

public class SparkGISConfig
{
    // // Jar path
    // public static String jarPath;
    // HDFS configurations
    public static String hdfsNameNodeIP;
    public static String hdfsAlgoData; // OPTIONAL
    public static String hdfsHMResults; // OPTIONAL

    static{
	InputStream inputStream = null;
	try {
	    //jarPath = SparkGISConfig.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
	    Properties prop = new Properties();
	    inputStream =
	    	SparkGISConfig.class.getClassLoader().getResourceAsStream("sparkgis.properties");
	    prop.load(inputStream);

	    // HDFS configurations
	    hdfsNameNodeIP = prop.getProperty("hdfs-name-node-ip");
	    hdfsAlgoData = prop.getProperty("hdfs-algo-data");
	    hdfsHMResults = prop.getProperty("hdfs-hm-results");
	    inputStream.close();
	} catch (Exception e) {
	    e.printStackTrace();
	    System.exit(-1);
	} finally{
	    try{
		inputStream.close();
	    }catch(Exception e){
		System.out.println("No stream to close");
	    }
	}
    }
}
