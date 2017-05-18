package driver;
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
/* Spark imports */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
/* Local imports */
import sparkgis.coordinator.SparkGISJobConf;
import sparkgis.coordinator.SparkGISContext;
import sparkgis.coordinator.functions.HeatMap;
import sparkgis.coordinator.functions.SpatialJoin;
import sparkgis.enums.HMType;
import sparkgis.enums.Predicate;
import sparkgis.data.DataConfig;
import sparkgis.SparkGISConfig;
import sparkgis.enums.PartitionMethod;


public class SparkGISBMIHeatMap
{
    /**
     * Command line arguments:
     * -a Comma separated list of algos [yi-algorithm-v1 | yi-algorithm-v11]
     * -c Comma separated list of caseIDs
     * -m Heatmap type [Dice] - Deafult
     * -p Partition-type [Jaccard]
     * -s Partition tile size
     */

    public static void main(String[] args)
    {
	/* Set default values */
	String jobID = null;
	Predicate predicate = Predicate.INTERSECTS;
	HMType hmType = HMType.JACCARD;
	PartitionMethod partitionMethod = PartitionMethod.FIXED_GRID_HM;
	int partitionSize = 32;
	/******************************************************/
	final CommandLineParser parser = new BasicParser();
	final Options options = new Options();
	options.addOption("u", "uid", true, "32-bit unique ID");
	options.addOption("a", "algos", true, "Comma separated list of algorithms [yi-algorithm-v1 | yi-algorithm-v11]");
	options.addOption("c", "caseids", true, "Comma separated list of caseIDs");
	options.addOption("m", "metric", true, "Metric type [jaccard|dice|tile_dice] Default:jaccard");
	options.addOption("p", "partitioner", true, "Distributed partitioner [fixed_grid|step] Default:fixed_grid");
	options.addOption("s", "tilesize", true, "Partition tile size Default: 32 would create 1024 tiles");
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
		if(mType.equalsIgnoreCase("dice")) hmType = HMType.DICE;
		else if(mType.equalsIgnoreCase("tile_dice")) hmType = HMType.TILEDICE;
	    }
	    /* Partition size */
	    final String pSize = getOption('s', commandLine);
	    partitionSize = pSize == ""? 32 : Integer.parseInt(pSize);

	    final List<String> caseIDs = Arrays.asList(caseIDcsv.split(","));
	    final List<String> algos = Arrays.asList(algosCsv.split(","));

	    /* Log job options */
	    System.out.println("JobID:\t" + jobID);
	    System.out.println("HDFS Namenode:\t" + SparkGISConfig.hdfsNameNodeIP);

	    System.out.println("caseIDs:");
	    for (String caseID:caseIDs)
		System.out.println("\t" + caseID);
	    System.out.println("Algos:");
	    for (String algo:algos)
		System.out.println("\t" + algo);
	    System.out.println("Predicate:\t" + predicate.value);
	    System.out.println("Metric Type:\t" + hmType.value);
	    System.out.println("Partition size:\t" + partitionSize);

	    
	    /* Initialize SparkConf */
	    SparkConf conf = new SparkConf().setAppName("SparkGIS-HeatMap");
	    /* set properties */
	    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	    conf.set("textinputformat.record.delimiter", "\n");
	    conf.set("spark.kryo.registrator", sparkgis.KryoClassRegistrator.class.getName());
	    /* Initialize SparkGISJobConf */
	    SparkGISJobConf spgConf =
		new SparkGISJobConf().
		setJobID(jobID).
		setBatchFactor(8).
		setDelimiter("\t").
		setSpatialObjectIndex(1).
		setPartitionSize(partitionSize).
		setPartitionMethod(partitionMethod);

	    /* Initialize SparkGISContext */
	    SparkGISContext spgc = new SparkGISContext(conf, spgConf);
	    
	    /* Execute job */
	    String res = HeatMap.execute(spgc, algos, caseIDs, predicate, hmType);
	    System.out.println("HeatMap results stored at: " + res);

	    spgc.stop();
	}
	catch(ParseException e){
	    e.printStackTrace();
	    formatter.printHelp("SparkGIS", options);
	}
	catch(Exception e){e.printStackTrace();}
	/******************************************************/
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
