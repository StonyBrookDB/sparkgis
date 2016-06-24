package sparkgis;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Properties;

public class SparkGISConfig
{
    // Jar path
    public static Boolean ifUpload;
    public static String jarPath;
    // HDFS configurations    
    public static String hdfsCoreSitePath;
    public static String hdfsHdfsSitePath;
    public static String hdfsNameNodeIP;
    public static String hdfsAlgoData; // OPTIONAL
    public static String hdfsHMResults; // OPTIONAL
    // MongoDB configurations
    public static String mongoHost;
    public static int mongoPort;
    public static String mongoDB;
    public static String collection_name;
    public static String collection_name_temp;
    
    public static int partition_size  ;
    
    /**
     * Considering maven directory structure
     * src
     * src/main/java/ ...
     * resources
     * resources/sparkgis.properties
     * target
     * terget/spark-gis-1.0-shaded.jar
     */
    static{
	try {
	    jarPath = SparkGISConfig.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
	    File jarFile = new File(jarPath);
	    String jarDir = jarFile.getParentFile().getPath();
	    Properties prop = new Properties();
	    FileInputStream inputStream = new FileInputStream(jarDir + "/../resources/sparkgis.properties");   
	    if (inputStream == null){
		System.out.println("Properties file Not found!!!");
		System.exit(1);
	    }
	    prop.load(inputStream);
	    
	    // HDFS configurations
	    hdfsCoreSitePath = prop.getProperty("hdfs-coresite-path");
	    hdfsHdfsSitePath = prop.getProperty("hdfs-hdfssite-path");
	    hdfsNameNodeIP = prop.getProperty("hdfs-name-node-ip");
	    hdfsAlgoData = prop.getProperty("hdfs-algo-data");
	    hdfsHMResults = prop.getProperty("hdfs-hm-results");
	    // MongoDB configurations
	    mongoHost = prop.getProperty("mongo-host");
	    mongoPort = Integer.parseInt(prop.getProperty("mongo-port"));
	    mongoDB = prop.getProperty("mongo-db");
        collection_name = prop.getProperty("mongo-collection");
        collection_name_temp = prop.getProperty("mongo-collection_temp");


      partition_size = Integer.parseInt(prop.getProperty("partition-size"));
      System.out.println("part_size: "+partition_size);
	    inputStream.close();
	} catch (Exception e) {e.printStackTrace();} 
    }
}
