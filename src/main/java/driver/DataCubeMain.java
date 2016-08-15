package driver;

import java.io.File;
import java.util.List;
import org.apache.commons.configuration.XMLConfiguration;

import java.util.Set;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import com.google.common.collect.Sets;

import sparkgis.SparkGISConfig;

import java.util.*;





/* Command Line Interface imports */
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLineParser;









public class DataCubeMain {

private static String configPath = "/../resources/config.xml";

public static int dimCount = 0;

public static void main(String[] args){


        List<String> caseIDs=null;
        final CommandLineParser parser = new BasicParser();
        final Options options = new Options();

        options.addOption("c", "caseids", true, "Comma separated list of caseIDs");
        options.addOption("d", "inputdb", true, "db");
        options.addOption("q", "inputcollection", true, "collection");
        options.addOption("m", "dimension", true, "dimension");
        options.addOption("n", "range", true, "range");

        HelpFormatter formatter = new HelpFormatter();

        try{
                final CommandLine commandLine = parser.parse(options, args);


                if (commandLine.hasOption('c')) {

                        String caseIDcsv = commandLine.getOptionValue('c');

                        System.out.println("caseIDcsv:  "+caseIDcsv);
                        caseIDs = Arrays.asList(caseIDcsv.split(","));
                }

                if (commandLine.hasOption('d')) {
                        SparkGISConfig.db =  commandLine.getOptionValue('d');
                        System.out.println("database:  "+SparkGISConfig.db);
                }

                if (commandLine.hasOption('q')) {
                        SparkGISConfig.collection = commandLine.getOptionValue('q');
                        System.out.println("collection:  "+SparkGISConfig.collection);
                }

                if (commandLine.hasOption('m')) {
                        SparkGISConfig.dimension_str = commandLine.getOptionValue('m');
                        System.out.println("dimension_str:  "+ SparkGISConfig.dimension_str);


                }


                if (commandLine.hasOption('n')) {
                        SparkGISConfig.query_range = commandLine.getOptionValue('n');
                        System.out.println("  SparkGISConfig.query_range:  "+   SparkGISConfig.query_range);


                }






        }
        catch(Exception e)
        {
                System.out.println(e);
        }













        DCTest test = new DCTest();
        //for (dimCount=2; dimCount<=8; dimCount+=2){
        // for (dimCount=8; dimCount<=8; dimCount+=2) {
        //         //test.start(
        //         test.startStreaming(
        //                 dimCount, // number of dimensions
        //                 1000, // number of buckets for each dimension
        //                 caseIDs // corresponds to the number of objects
        //                 );
        // }


        test.startStreaming_2(  caseIDs );

        //DataCubeShell.shell();

        //createConfig();

        //readConfig();

        // List<String> l1 = Arrays.asList("A","B","C");
        // List<String> l2 = Arrays.asList("1","2");
        // List<String> l3 = Arrays.asList("a","b");

        // List<List<String>> lists = new ArrayList<List<String>>();
        // lists.add(l1);
        // lists.add(l2);
        // lists.add(l3);

        // Set<List<String>> combs = getCombinations(lists);
        // for(List<String> list : combs) {
        //     System.out.println(list.toString());
        // }


        // dc.addDimension(PropertyName.ROUNDNESS, bucketCount);
        // dc.addDimension(PropertyName.FLATNESS, bucketCount);
        // dc.addDimension(PropertyName.PERIMETER, bucketCount);
        // dc.addDimension(PropertyName.EQUIVALENT_SPHERICAL_RADUIS, bucketCount);
        // dc.addDimension(PropertyName.EQUIVALENT_SPHERICAL_PERIMETER, bucketCount);
        // dc.addDimension(PropertyName.EQUIVALENT_ELLIPSOID_DIAMETER0, bucketCount);

        // dc.mapIndex(0,0);
        // dc.mapIndex(0,1);
        // dc.mapIndex(1,0);
        // dc.mapIndex(1,1);
        // dc.mapIndex(2,0);
        // dc.mapIndex(2,1);

        // dc.mapIndex(0, 0, 0);
        // dc.mapIndex(0, 0, 1);
        // dc.mapIndex(0, 1, 0);
        // dc.mapIndex(0, 1, 1);
        // dc.mapIndex(1, 0, 0);
        // dc.mapIndex(1, 0, 1);
        // dc.mapIndex(1, 1, 0);
        // dc.mapIndex(1, 1, 1);


        // dc.addDimension(PropertyName.ROUNDNESS, 2.0);
        // dc.buildCube();
}

// private static void testGuava(){
//  List<String> l1 = {"A", "B", "C"};
//  List<String> l2 = {"1", "2"};
//  List<String> l3 = {"a", "b", "c"};

//  Set<List<String>> result = Sets.cartesianProduct(l1, l2, l3);

//  System.out.println(result);
// }

// public static <T> Set<List<T>> getCombinations(List<List<T>> lists) {
//  Set<List<T>> combinations = new HashSet<List<T>>();
//  Set<List<T>> newCombinations;

//  int index = 0;

//  // extract each of the integers in the first list
//  // and add each to ints as a new list
//  for(T i: lists.get(0)) {
//      List<T> newList = new ArrayList<T>();
//      newList.add(i);
//      combinations.add(newList);
//  }
//  index++;
//  while(index < lists.size()) {
//      List<T> nextList = lists.get(index);
//      newCombinations = new HashSet<List<T>>();
//      for(List<T> first: combinations) {
//    for(T second: nextList) {
//        List<T> newList = new ArrayList<T>();
//        newList.addAll(first);
//        newList.add(second);
//        newCombinations.add(newList);
//    }
//      }
//      combinations = newCombinations;

//      index++;
//  }

//  return combinations;
// }

private static void readConfig(){
        try{
                String jarPath = DataCubeMain.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
                File jarFile = new File(jarPath);
                String jarDir = jarFile.getParentFile().getPath();

                XMLConfiguration config = new XMLConfiguration(jarDir + configPath);

                List<String> names = config.getList("datacube.dimensions.dimension.name");
                for (String str : names)
                        System.out.println("Name:" + str);

                List<String> resolutions = config.getList("datacube.dimensions.dimension.resolution");
                for (String res : resolutions)
                        System.out.println("Res:" + Double.parseDouble(res));

        }catch(Exception e) {e.printStackTrace(); }
}

private static void createConfig(){
        try{
                String jarPath = DataCubeMain.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
                File jarFile = new File(jarPath);
                String jarDir = jarFile.getParentFile().getPath();

                XMLConfiguration configCreate = new XMLConfiguration();
                configCreate.setFileName(jarDir + configPath);
                configCreate.addProperty("datacube.dimensions.dimension.name", "area");
                configCreate.addProperty("datacube.dimensions.dimension.resolution", "10");
                configCreate.addProperty("datacube.dimensions.dimension.name", "elongation");
                configCreate.addProperty("datacube.dimensions.dimension.resolution", "0.5");
                configCreate.save();
        }catch (Exception e) {e.printStackTrace(); }
}
}
