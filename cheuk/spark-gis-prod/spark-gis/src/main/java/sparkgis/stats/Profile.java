package sparkgis.stats;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.TimeUnit;
// /* for asyncHeatMap */
import java.util.List;
import java.util.ArrayList;
import sparkgis.SparkGIS;
//import sparkgis.data.RawData;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.enums.HMType;
import sparkgis.enums.IO;
import sparkgis.executionlayer.SparkPrepareData;
//import sparkgis.storagelayer.mongodb.MongoDBDataAccess;
//import sparkgis.storagelayer.hdfs.HDFSDataAccess;

public class Profile
{
    //private static AtomicInteger processed = new AtomicInteger();
    private static int processed = 0;
    private static int total = 0;
    // public final String caseID;

    // public long mongoTime1; // [IO] MongoDB access time for algorithm 1
    // public long mongoTime2; // [IO] MongoDB access time for algorithm 2

    // public long prepDataTime1;
    // public long prepDataTime2;
    
    // public long objects1;
    // public long objects2;

    // public long sparkExeTime; // [EXE] Time spend in Spark for heatmap generation
    
    // private boolean title = false;

    // public Profile(String caseID){
    // 	this.caseID = caseID;
    // }

    public static void heatmaps(String jID, String collection,IO spIn, List<String> caseIDs, List<String> algos, Predicate predicate, HMType hmType, int partitionSize, IO spOut, String result_analysis_exe_id){
	long start = System.nanoTime();
	sparkgis.SparkGISMain.callHeatMap(jID, collection,spIn, caseIDs, algos, predicate, hmType, partitionSize, spOut, result_analysis_exe_id);
	long exeTime = System.nanoTime() - start; 
	try {
    	    PrintWriter out = 
    		new PrintWriter(new BufferedWriter(new FileWriter("logs/Report.log", true)));
	    out.print("Algorithms: \t\t");
	    for (String algo:algos){
		out.print(algo + ", ");
	    }
	    out.println("");
	    out.println("Case IDs count: \t" + caseIDs.size());
	    out.println("Partition Size: \t" + partitionSize);
	    out.println("Input: \t\t\t" + (spIn==IO.HDFS?"HDFS":"MongoDB"));
	    out.println("Output: \t\t" + (spOut==IO.HDFS?"HDFS":"MongoDB"));
	    out.println("Total Time: \t\t" + TimeUnit.SECONDS.convert(exeTime, TimeUnit.NANOSECONDS) + " s");
	    out.println("*****************************************************");
	    out.close();
	}catch(Exception e){e.printStackTrace();}
    }
    
    private static ArrayList<String> logs = new ArrayList<String>();
    public static void log2(String str){
	logs.add(str);
    }
    public static void yank(){
	try {
    	    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("logs/Partition-Report.log", true)));
	    for (String str:logs){
		out.println(str);
	    }
	    out.close();
	}catch(Exception e){e.printStackTrace();}
    }

    
    /**
     * Wrapper function for profiling
     * 1-thread:  Linear execution
     * 2-threads: Algo1 and Algo2 parallelised only
     * 3-threads: Separate threads for each image. Algo1 and Algo2 parallelized
     */
    // public static void asyncHeatMaps(final SparkGIS sparkgis, List<String> algos, List<String> caseIDs, Predicate predicate, HMType hmType, int threadCount){
    	
    // 	total = caseIDs.size();
	
    // 	long start = 0;
    // 	/* Sequential - No parallelism */
    // 	if (threadCount == 1){
    // 	    start = System.nanoTime();
    // 	    SparkGISExecutionEngine engine = new SparkGISExecutionEngine();
    // 	    for (String caseID : caseIDs){
    // 		// prepare data
    // 		DataConfig config1 = engine.prepareData(sparkgis.inputSrc, caseID, algos.get(0));
    // 		DataConfig config2 = engine.prepareData(sparkgis.inputSrc, caseID, algos.get(1));
    // 		// generate heatmap
    // 		engine.generateHeatMap(config1, config2, predicate, hmType, sparkgis.outDest);
    // 	    }
    // 	}
    // 	else if (threadCount == 2){
    // 	    start = System.nanoTime();
    // 	    for (String caseID : caseIDs){
    // 		/* Algo1 and Algo2 parallelised in seperate threads */
    // 		(new AsyncGenerateHeatMap(sparkgis.inputSrc, caseID, algos.get(0), algos.get(1), predicate, hmType, sparkgis.outDest)).call();
    // 	    }
    // 	}
    // 	else{
    // 	    start = System.nanoTime();
    // 	    SparkGISExecutionEngine engine = new SparkGISExecutionEngine();
    // 	    engine.asyncGenerateHeatMaps(sparkgis.inputSrc, caseIDs, algos, predicate, hmType, (threadCount/2), sparkgis.outDest);
    // 	}
    	
    // 	// System.out.print( caseIDs.size() + " Heat Maps generated in ");
    // 	// System.out.print(TimeUnit.SECONDS.convert(
    // 	// 					  System.nanoTime() - start, 
    // 	// 					  TimeUnit.NANOSECONDS
    // 	// 					  ) 
    // 	// 		 + " s\n");

    // 	try {
    // 	    PrintWriter out = 
    // 		new PrintWriter(new BufferedWriter(new FileWriter("logs/Report.log", true)));
   // 	    out.println("*****************************************************");
    // 	    out.println(System.currentTimeMillis());
    // 	    out.println("Parallelism:\t" + sparkgis.sc.defaultParallelism());
    // 	    out.println("Threads count:\t" + threadCount);
    // 	    out.println("CaseID count:\t" + caseIDs.size());
    // 	    out.println("Algorithms:\t" + algos.get(0) + ", " + algos.get(1));
    // 	    out.println("Total Time:\t" + TimeUnit.SECONDS.convert(
    // 								   System.nanoTime() - start, 
    // 								   TimeUnit.NANOSECONDS
    // 								   ) + " s");
    // 	    // long io = 0;
    // 	    // long exe1 = 0;
    // 	    // long exehm = 0;
    // 	    long obj = 0;
    // 	    for (Detail d : details){
    // 	    	//if (d.isTime){
    // 	    	// if (d.field.startsWith("[IO]"))
    // 	    	//     io += d.value;
    // 	    	// else if (d.field.startsWith("[EXE-1]"))
    // 	    	//     exe1 += d.value;
    // 	    	// else if(d.field.startsWith("[EXE-HM]"))
    // 	    	//     exehm += d.value;
    // 	    	// else
    // 		if (d.field.startsWith("[Obj]"))
    // 	    	    obj += d.value;
    // 		//out.println(d.field + "\t"+TimeUnit.SECONDS.convert(d.value, TimeUnit.NANOSECONDS)+ " s");
		
    // 		// else
    // 	    	//    out.println(d.field + "\t" + d.value);
    // 	    }
    // 	    // out.println("Total IO:\t" + TimeUnit.SECONDS.convert(io, TimeUnit.NANOSECONDS) + " s");
    // 	    // out.println("Total Exe-1:\t" + TimeUnit.SECONDS.convert(exe1, TimeUnit.NANOSECONDS) + " s");
    // 	    // out.println("Total Exe-HM:\t" + TimeUnit.SECONDS.convert(exehm, TimeUnit.NANOSECONDS) + " s");
    // 	    out.println("Total Objs:\t" + obj);
    // 	    out.println("*****************************************************");
    // 	    out.close();
    // 	} catch (Exception e) {e.printStackTrace();}
    // }
    
    private static ArrayList<Detail> details = new ArrayList<Detail>();
    public static void log(String field, long value){
    	details.add(new Detail(field, value));
    }

    public static void printProgress(){
	//System.out.print("\rProcessed: " + processed.getAndIncrement() + "/" + total);
	System.out.print("\rProcessed: " + (processed++) + "/" + total);
    }
    
    private static class Detail{
    	final String field;
    	final long value;
    	boolean isTime = true;
    	public Detail(String field, long value){
    	    this.field = field;
    	    this.value = value;
    	    if (field.startsWith("[Obj]"))
    		isTime = false;
    	}
    }
    
    // public void yank(String file){
    // 	try {
    // 	    PrintWriter out = 
    // 		new PrintWriter(new BufferedWriter(new FileWriter("logs/"+file+".log", true)));
    // 	    out.println(this.toString());
    // 	    out.close();
    // 	} catch (Exception e) {e.printStackTrace();}
    // 	// csv
    // 	try {
    // 	    PrintWriter out = 
    // 		new PrintWriter(new BufferedWriter(new FileWriter("logs/"+file+".tsv", true)));
    // 	    //if (!title){
    // 		//out.println("caseID, Algo1-objects, Algo2-objects, Algo1-IO, Algo2-IO, Algo1-DataPrep, Algo2-DataPrep, Heatmap");
    // 	    //	title = true;
    // 	    //}
    // 	    out.println(this.toTSV());
    // 	    out.close();
    // 	} catch (Exception e) {e.printStackTrace();}
    // }

    // /**
    //  * caseID, Algo1 Objects, Algo2 Objects, IO Algo1, IO Algo2, Prep Algo1, Prep Algo2, Heatmap
    //  */
    // private String toTSV(){
    // 	return "" +
    // 	    caseID + "\t" +
    // 	    objects1 + "\t" +
    // 	    objects2 + "\t" +
    // 	    TimeUnit.SECONDS.convert(mongoTime1, TimeUnit.NANOSECONDS) + "\t" +
    // 	    TimeUnit.SECONDS.convert(mongoTime2, TimeUnit.NANOSECONDS) + "\t" +
    // 	    TimeUnit.SECONDS.convert(prepDataTime1, TimeUnit.NANOSECONDS) + "\t" +
    // 	    TimeUnit.SECONDS.convert(prepDataTime2, TimeUnit.NANOSECONDS) + "\t" +
    // 	    TimeUnit.SECONDS.convert(sparkExeTime, TimeUnit.NANOSECONDS)
    // 	    ;
    // 	    }
    // public String toString(){
    // 	return
    // 	    "\n**************************************" + "\n" +
    // 	    "caseID: " + this.caseID + "\n" +
    // 	    "[IO] MongoDB Access Time for Algo1: " + TimeUnit.SECONDS.convert(mongoTime1, TimeUnit.NANOSECONDS) + " s for Objects: " + objects1 + "\n" +
    // 	    "[IO] MongoDB Access Time for Algo2: " + TimeUnit.SECONDS.convert(mongoTime2, TimeUnit.NANOSECONDS) + " s for Objects: " + objects2 + "\n" +
    // 	    "[SPARK] Prepare data time for Algo1: " + TimeUnit.SECONDS.convert(prepDataTime1, TimeUnit.NANOSECONDS) + " s\n" +
    // 	    "[SPARK] Prepare data time for Algo2: " + TimeUnit.SECONDS.convert(prepDataTime2, TimeUnit.NANOSECONDS) + " s\n" +
    // 	    "[SPARK] HeatMap generation time: " + TimeUnit.SECONDS.convert(sparkExeTime, TimeUnit.NANOSECONDS) + " s\n" +
    // 	    "**************************************";
    // }
}
