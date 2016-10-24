package sparkgis;
/* Java imports */
import java.io.*;
import java.util.List;
import java.util.UUID;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
/* Command Line Interface imports */
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
/* Local imports */
import sparkgis.enums.IO;
import sparkgis.enums.Predicate;
import sparkgis.enums.HMType;
import sparkgis.data.DataConfig;
import sparkgis.stats.Profile;
import sparkgis.io.MongoToHDFS;
import sparkgis.io.ISparkGISIO;
import sparkgis.io.hdfs.HDFSDataAccess;
import sparkgis.io.mongodb.MongoDBDataAccess;
import sparkgis.SparkGISConfig;


public class SparkGISMain
{
    /**
     * Command line arguments: 
     * -a Comma separated list of algos [yi-algorithm-v1 | yi-algorithm-v11]
     * -c Comma separated list of caseIDs
     * -d Heatmap type [Dice] - Deafult
     * -j Heatmap-type [Jaccard] 
     */
    
    public static void main(String[] args) 
    {	    
	System.out.println(SparkGISConfig.hdfsCoreSitePath);
	System.out.println(SparkGISConfig.hdfsHdfsSitePath);
	System.out.println(SparkGISConfig.hdfsNameNodeIP);
	
	System.out.println(SparkGISConfig.mongoHost);
	System.out.println(SparkGISConfig.mongoPort);
	
	String jobID = null;
	Predicate predicate = Predicate.INTERSECTS;
	HMType hmType = null;
	IO spIn = null;
	IO spOut = null;
	String result_analysis_exe_id = null;

	/******************************************************/
	final CommandLineParser parser = new BasicParser();
	final Options options = new Options();
	options.addOption("l", "upload", true, "upload_files");
	options.addOption("u", "uid", true, "32-bit unique ID");
	options.addOption("a", "algos", true, "Comma separated list of algorithms [yi-algorithm-v1 | yi-algorithm-v11]");
	options.addOption("c", "caseids", true, "Comma separated list of caseIDs");
	options.addOption("i", "input", true, "Input source [hdfs|mongodb]");
	options.addOption("o", "output", true, "Output destination [hdfs|mongodb|client]");

	options.addOption("q", "inputdb", true, "Input source [hdfs|mongodb]");
	options.addOption("w", "inputcollection", true, "Output destination [hdfs|mongodb|client]");

	options.addOption("z", "outputdb", true, "Input source [hdfs|mongodb]");
	options.addOption("x", "outputcollection", true, "Output destination [hdfs|mongodb|client]");

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
		if(mType.equalsIgnoreCase("jaccard")) hmType = HMType.JACCARD;
		else if(mType.equalsIgnoreCase("dice")) hmType = HMType.DICE;
		else if(mType.equalsIgnoreCase("tile_dice")) hmType = HMType.TILEDICE;
		else hmType = HMType.JACCARD;
	    }

	    if (commandLine.hasOption('l')){
		String tmp = getOption('l', commandLine);

		if(tmp.equalsIgnoreCase("yes"))
		    {
			spIn = IO.MONGODB;
			spOut = IO.MONGODB;

			SparkGISConfig.input_mongoDB = SparkGISConfig.temp_mongoDB;
			SparkGISConfig.input_collection_name = SparkGISConfig.temp_collection_name;
			SparkGISConfig.output_mongoDB =  SparkGISConfig.temp_mongoDB;
			SparkGISConfig.output_collection_name = SparkGISConfig.temp_collection_name;
		    }			
	    }
	    else{
		/* IO specifics */
		if (commandLine.hasOption('i')){
		    String in = commandLine.getOptionValue('i');		    
		    if (in.equalsIgnoreCase("hdfs")) spIn = IO.HDFS;
		    else if (in.equalsIgnoreCase("mongodb")) {
			if (commandLine.hasOption('q')  &&   commandLine.hasOption('w')      ){
			    
			    String inputdb = commandLine.getOptionValue('q');
			    String inputcol = commandLine.getOptionValue('w');
			    
			    SparkGISConfig.input_mongoDB = inputdb;
			    SparkGISConfig.input_collection_name = inputcol;
			    
			    System.out.println("inputdbname: "+SparkGISConfig.input_mongoDB );
			    System.out.println("inputdbcollection:  "+SparkGISConfig.input_collection_name);
			}			
			spIn = IO.MONGODB;
		    }
		    else throw new ParseException("Invalid input source");
		}
		else throw new ParseException("Invalid input source");
		if (commandLine.hasOption('o')){
		    String out = commandLine.getOptionValue('o');
		    if (out.equalsIgnoreCase("hdfs")) spOut = IO.HDFS;
		    else if (out.equalsIgnoreCase("mongodb")){

			if (commandLine.hasOption('q')  &&   commandLine.hasOption('w')      ){

			    String outputdb = commandLine.getOptionValue('z');
			    String outputcol = commandLine.getOptionValue('x');

			    SparkGISConfig.output_mongoDB = outputdb;
			    SparkGISConfig.output_collection_name = outputcol;

			    System.out.println("outputdbname: "+SparkGISConfig.output_mongoDB );
			    System.out.println("outputdbcollection:  "+SparkGISConfig.output_collection_name);
			}
			spOut = IO.MONGODB;
			// MongoDB output requires further values to be specified
		    
		    }
		    else if (out.equalsIgnoreCase("client")) spOut = IO.CLIENT;
		    else throw new ParseException("Invalid output destination");
		}
		else throw new ParseException("Invalid output destination");

	    }
	    /* Result config */
	    if (commandLine.hasOption('r')){
		result_analysis_exe_id = commandLine.getOptionValue('r');
	    }else throw new ParseException("result_analysis_exe_id not specified");
	    final List<String> caseIDs = Arrays.asList(caseIDcsv.split(","));
	    final List<String> algos = Arrays.asList(algosCsv.split(","));
	    int partitionSize = SparkGISConfig.partition_size;


	    /* print options */
	    System.out.println("Starting Job ...");
	    System.out.println("JobID:\t" + jobID);
	    System.out.println("caseIDs:");
	    for (String caseID:caseIDs)
		System.out.println("\t" + caseID);
	    System.out.println("Algos:");
	    for (String algo:algos)
		System.out.println("\t" + algo);
	    System.out.println("Predicate:\t" + predicate.value);
	    System.out.println("Metric Type:\t" + hmType.value);
	    System.out.println("Partition size:\t" + partitionSize);
	    System.out.println("Input:\t" + ((spIn==IO.HDFS)?"hdfs":"mongo"));
	    System.out.println("Output:\t" + ((spOut==IO.MONGODB)?"mongo":"hdfs"));

	    callHeatMap(jobID, spIn, caseIDs, algos, predicate, hmType, partitionSize, spOut, result_analysis_exe_id);	    
	}
	catch(ParseException e){
	    e.printStackTrace();
	    formatter.printHelp("SparkGIS", options);
	}
	catch(Exception e){e.printStackTrace();}
	/******************************************************/
    } 
    /* function to instentiate spark and generate heatmaps */
    public static void callHeatMap(String jID, IO in, List<String> caseIDs, List<String> algos, Predicate predicate, HMType hmType, int partitionSize, IO out, String result_analysis_exe_id){
	    
	boolean returnResults = false;
	/* HDFS custom configuration */
 	// final String coreSitePath = SparkGISConfig.hdfsCoreSitePath;
	// final String hdfsSitePath = SparkGISConfig.hdfsHdfsSitePath;
 
	// String hdfs_name_node_ip =  SparkGISConfig.hdfsNameNodeIP;
	// String hdfsPrefix = "hdfs://"+  hdfs_name_node_ip         +"/user/fbaig/";
	// final String dataDir = hdfsPrefix + "new-data/";
	// final String outDir = hdfsPrefix + "results/"+jID+"/";

	// final HDFSDataAccess hdfsInOut = new HDFSDataAccess(
	// 						 coreSitePath, 
	// 						 hdfsSitePath, 
	// 						 dataDir, 
	// 						 outDir
	// 						 );

	/* use default configurations specified in resources/sparkgis.properties */
	final HDFSDataAccess hdfsInOut = new HDFSDataAccess();
	hdfsInOut.appendResultsDir(jID);
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
	
	/* Initialize SparkGIS with input source and output destination */
	final SparkGIS spgis = new SparkGIS(spIn, spOut);
	spgis.heatMaps(jID,
		       algos, 
		       caseIDs, 
		       predicate,
		       hmType,
		       partitionSize,
		       result_analysis_exe_id
		       );
	/* shutdown this spark context */
	SparkGIS.sc.stop();
	
	if (returnResults)
	    System.out.println("Results are stored at: " + hdfsInOut.getResultsDir());
    }
   
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
	return "";
    }
}
