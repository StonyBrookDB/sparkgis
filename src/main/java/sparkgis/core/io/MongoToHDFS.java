package sparkgis.core.io;

// /* Java imports */
import java.io.*;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
// /* Local imports */
import sparkgis.core.data.Polygon;
import sparkgis.core.data.MongoPolygon;
// import sparkgis.data.Polygon;
import sparkgis.SparkGISConfig;
import sparkgis.core.io.mongodb.ReadMongoSplit;
import sparkgis.core.io.hdfs.HDFSDataAccess;

public class MongoToHDFS
{
    private final int threadCount = 2;
    private final String host = SparkGISConfig.mongoHost;
    private final int port = SparkGISConfig.mongoPort;
    private final String dbName = SparkGISConfig.mongoDB;

    public void execute(String algo1, String algo2, List<String> caseIDs){
	// create a log
	PrintWriter out = null;
	try {
    	    out = new PrintWriter(new BufferedWriter(new FileWriter("logs/IO.log", true)));
	}catch(Exception e){e.printStackTrace();}
	
	System.out.println(caseIDs.size());
	
	final ExecutorService exeService = Executors.newFixedThreadPool(threadCount);
    	List<Future> futures  = new ArrayList<Future>();
    	for (String caseID : caseIDs){
    	    futures.add(exeService.submit(new CopyData(algo1, caseID, out)));
    	    futures.add(exeService.submit(new CopyData(algo2, caseID, out)));
    	}
    	// wait for all threads to finish
    	for (Future f : futures){
    	    try{
    		f.get();
    	    }catch(Exception e){e.printStackTrace();}
    	}
    }
        
    class CopyData implements Runnable{
    	private final String algo;
    	private final String caseID;
	private final PrintWriter out;
    	public CopyData(String algo, String caseID, PrintWriter out){
    	    this.algo = algo;
    	    this.caseID = caseID;
	    this.out = out;
    	}
    	public void run(){
    	    HDFSDataAccess hdfs = new HDFSDataAccess();
	    // check if file already downloaded
	    if (hdfs.fileExists(algo+"/"+caseID))
		return;
	    LinkedHashMap<String, Object> params = new LinkedHashMap<String, Object>();
	    params.put("db", dbName);
	    params.put("algorithm", algo);
	    params.put("caseID", caseID);
	    ReadMongoSplit mongo = new ReadMongoSplit(host, port, params, 0, MongoPolygon.class);
	    List<Polygon> data = mongo.getData((long)0);	
	    // write data to HDFS
	    hdfs.writePolygons(data, algo+"/"+caseID);
	    try{
		out.println(algo + "-" + caseID + ":" + data.size() + " Done!");
		out.flush();
	    }catch(Exception e){e.printStackTrace();}
    	}
    }
}
