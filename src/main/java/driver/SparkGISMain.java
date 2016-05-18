package driver;
/* Java imports */
import java.io.*;
import java.util.List;
import java.util.UUID;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
/* Command Line Interface imports */
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLineParser;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.pia.HMType;
import sparkgis.stats.Profile;
import sparkgis.core.enums.IO;
import sparkgis.SparkGISConfig;
import sparkgis.core.io.MongoToHDFS;
import sparkgis.core.io.ISparkGISIO;
import sparkgis.core.enums.Predicate;
import sparkgis.core.enums.Delimiter;
import sparkgis.core.data.DataConfig;
import sparkgis.core.io.hdfs.HDFSDataAccess;
import sparkgis.core.io.local.LocalDataAccess;
import sparkgis.core.io.mongodb.MongoDBDataAccess;


public class SparkGISMain
{

    //private static final int imagesToProcess = 400;
    
    /**
     * Command line arguments: 
     * -a Comma separated list of algos [yi-algorithm-v1 | yi-algorithm-v11]
     * -c Comma separated list of caseIDs
     * -d Heatmap type [Dice] - Deafult
     * -j Heatmap-type [Jaccard] 
     */
    
    public static void main(String[] args) 
    {	
	String jobID = null;
	Predicate predicate = Predicate.INTERSECTS;
	HMType hmType = null;
	IO spIn = null;
	IO spOut = null;
	String result_analysis_exe_id = null;

	/******************************************************/
	final CommandLineParser parser = new BasicParser();
	final Options options = new Options();
	options.addOption("u", "uid", true, "32-bit unique ID");
	options.addOption("a", "algos", true, "Comma separated list of algorithms [yi-algorithm-v1 | yi-algorithm-v11]");
	options.addOption("c", "caseids", true, "Comma separated list of caseIDs");
	options.addOption("i", "input", true, "Input source [hdfs|mongodb]");
	options.addOption("o", "output", true, "Output destination [hdfs|mongodb|client]");
	options.addOption("m", "metric", true, "Metric type [jaccard|dice|tile_dice] Default:jaccard");
	options.addOption("r", "result_exe_id", true, "execution_analysis_id for result to show in caMicroscope");
	HelpFormatter formatter = new HelpFormatter();
	
	try{
	    final CommandLine commandLine = parser.parse(options, args);	    
	    /* Job ID */
	    if (commandLine.hasOption('u')){
		jobID = getOption('u', commandLine);
	    }
	    else
		jobID = UUID.randomUUID().toString();
	    /* list of algos */
	    final String algosCsv = getOption('a', commandLine);
	    /* List of caseIDs */
	    final String caseIDcsv = getOption('c', commandLine);
	    /* Heatmap metric type */
	    if (commandLine.hasOption('m')){
		String mType = commandLine.getOptionValue('m');
		if(mType.equals("jaccard")) hmType = HMType.JACCARD;
		else if(mType.equals("dice")) hmType = HMType.DICE;
		else if(mType.equals("tile_dice")) hmType = HMType.TILEDICE;
		else hmType = HMType.JACCARD;
	    }
	    /* IO specifics */
	    if (commandLine.hasOption('i')){
		String in = commandLine.getOptionValue('i');		    
		if (in.equals("hdfs")) spIn = IO.HDFS;
		else if (in.equals("mongodb")) spIn = IO.MONGODB;
		else throw new ParseException("Invalid input source");
	    }
	    else throw new ParseException("Invalid input source");
	    if (commandLine.hasOption('o')){
		String out = commandLine.getOptionValue('o');
		if (out.equals("hdfs")) spOut = IO.HDFS;
		else if (out.equals("mongodb")){
		    spOut = IO.MONGODB;
		    // MongoDB output requires further values to be specified
		    if (commandLine.hasOption('r')){
			result_analysis_exe_id = commandLine.getOptionValue('r');
		    }else throw new ParseException("result_analysis_exe_id not specified");
		}
		else if (out.equals("client")) spOut = IO.CLIENT;
		else throw new ParseException("Invalid output destination");
	    }
	    else throw new ParseException("Invalid output destination");
	    
	    final List<String> caseIDs = Arrays.asList(caseIDcsv.split(","));
	    final List<String> algos = Arrays.asList(algosCsv.split(","));
	    
	    // EXPERIMENT ONLY get caseID list
	    //final List<String> caseIDs = getCaseIDs("download/caseids.out");
	    // remove from list
	    //for (int i=caseIDs.size()-1; i>(10-1); --i)
	    //	caseIDs.remove(i);
	    
	    callHeatMap(jobID, spIn, caseIDs, algos, predicate, hmType, 512, spOut, result_analysis_exe_id);
	    
	    // for (int i=128; i<=512; i+=128){
	    // 	jobID = UUID.randomUUID().toString();
	    // 	Profile.heatmaps(jobID, caseIDs, algos, predicate, hmType, i);
	    //}
	    //Profile.heatmaps(jobID, spIn, caseIDs, algos, predicate, hmType, 512, spOut, result_analysis_exe_id);
	}
	catch(ParseException e){
	    e.printStackTrace();
	    formatter.printHelp("SparkGIS", options);
	}
	catch(Exception e){e.printStackTrace();}
	/******************************************************/

	System.out.println(SparkGISConfig.hdfsCoreSitePath);
	System.out.println(SparkGISConfig.hdfsHdfsSitePath);
	System.out.println(SparkGISConfig.hdfsNameNodeIP);
	
	System.out.println(SparkGISConfig.mongoHost);
	System.out.println(SparkGISConfig.mongoPort);
	System.out.println(SparkGISConfig.mongoDB);

	// IGNORE ABOVE
	// System.out.println("Calling Spatial Join");
	// callSpatialJoin();
	
    }

    public static void callSpatialJoin(){
	// relative to hdfs properties set in resources/sparkgis.properties
	String dataPath1 = "/user/fbaig/osm/europe.dat.2";
	String dataPath2 = "/user/fbaig/osm/planet.dat.1";
	String outPath = "/user/fbaig/osm/results/";
	//String dp = "data.txt";
	//final ISparkGISIO localInOut = new LocalDataAccess();
	final ISparkGISIO hdfsInOut = new HDFSDataAccess();
	final SparkGIS spgis = new SparkGIS(hdfsInOut, hdfsInOut);
	long start = System.nanoTime();
	spgis.spatialJoin(
			  dataPath1,
			  dataPath2,
			  Delimiter.PIPE,
			  Predicate.INTERSECTS,
			  512,
			  outPath
			  );
	
	// shutdown this spark context
	spgis.stop();
	
    }

    // function to instentiate spark and generate heatmaps
    public static void callHeatMap(String jID, IO in, List<String> caseIDs, List<String> algos, Predicate predicate, HMType hmType, int partitionSize, IO out, String result_analysis_exe_id){
	    
	boolean returnResults = false;
	// HDFS configuration
	// relative to hdfs properties set in SparkGISConfig
	final ISparkGISIO hdfsInOut = new HDFSDataAccess();
	final MongoDBDataAccess mongoInOut = new MongoDBDataAccess();
	ISparkGISIO spIn = null;
	ISparkGISIO spOut = null;
	// input source
	switch (in){
	case MONGODB:
	    spIn = mongoInOut;
	    break;
	case HDFS:
	    spIn = hdfsInOut;
	    break;
	default:
	    System.out.println("Invalid IO");
	    return;
	}
	// Output destination
	switch (out){
	case MONGODB:
	    spOut = mongoInOut;
	    break;
	case HDFS:
	    spOut = hdfsInOut;
	    break;
	case CLIENT:
	    spOut = hdfsInOut;
	    returnResults = true;
	    break;
	default:
	    System.out.println("Invalid IO");
	    return;
	}
	
	// Initialize SparkGIS with input source and output destination
	final SparkGIS spgis = new SparkGIS(spIn, spOut);
	spgis.heatMaps(
		       algos, 
		       caseIDs, 
		       predicate,
		       hmType,
		       partitionSize, 
		       result_analysis_exe_id
		       );
	// shutdown this spark context
	spgis.stop();
	
	if (returnResults)
	    System.out.println("Results are stored at: ");
    }
   
    // private static void checkFiles(List<String> caseIDs){
    // 	HDFSDataAccess hdfs = new HDFSDataAccess();
    // 	for(String caseID : caseIDs){
    // 	    if (!hdfs.fileExists(algo1+"/" + caseID))
    // 		System.out.println(algo1+"-"+caseID+" Doesnot exit !!!");
    // 	    if (!hdfs.fileExists(algo2+"/" + caseID))
    // 		System.out.println(algo2+"-"+caseID+" Doesnot exit !!!");
    // 	}
    // 	System.out.println("Done!!!");
    // }
    
    // private static void mongoToHDFS(List<String> caseIDs){
    // 	MongoToHDFS m2h = new MongoToHDFS();
    // 	m2h.execute(algo1, algo2, caseIDs);
    // }
   
    /**
     * Get caseID list to process
     */
    private static List<String> getCaseIDs(final String fileName){
    	List<String> images = new ArrayList<String>();
    	try{
    	    BufferedReader br = new BufferedReader(new FileReader(fileName));
    	    String line = null;
    	    while ((line = br.readLine()) != null)
    		images.add(line);
    	}catch(Exception e){e.printStackTrace();}
    	return images;
    }

    
    private static String getOption(final char option, final CommandLine commandLine) {
	if (commandLine.hasOption(option)) {
	    return commandLine.getOptionValue(option);
	}
	return "";//StringUtils.EMPTY;
    }
}
