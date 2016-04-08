package datacube;

import java.io.File;
import java.util.List;
import org.apache.commons.configuration.XMLConfiguration;

import java.util.Set;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import com.google.common.collect.Sets;

/* Local imports */
import datacube.data.PropertyName;

public class DataCubeMain{

    private static String configPath = "/../resources/config.xml";

    public static void main(String[] args){
	//DataCubeShell.shell();

	//createConfig();
	
	readConfig();

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

	DataCube dc = new DataCube();
	dc.addDimension(PropertyName.AREA, 1000);
	dc.addDimension(PropertyName.ELONGATION, 1000);
	dc.addDimension(PropertyName.ROUNDNESS, 1000);

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
	
	dc.buildCube();

	// dc.addDimension(PropertyName.ROUNDNESS, 2.0);
	// dc.buildCube();
    }

    // private static void testGuava(){
    // 	List<String> l1 = {"A", "B", "C"};
    // 	List<String> l2 = {"1", "2"};
    // 	List<String> l3 = {"a", "b", "c"};
	
    // 	Set<List<String>> result = Sets.cartesianProduct(l1, l2, l3);
	
    // 	System.out.println(result);
    // }

    public static <T> Set<List<T>> getCombinations(List<List<T>> lists) {
	Set<List<T>> combinations = new HashSet<List<T>>();
	Set<List<T>> newCombinations;
	
	int index = 0;
	
	// extract each of the integers in the first list
	// and add each to ints as a new list
	for(T i: lists.get(0)) {
	    List<T> newList = new ArrayList<T>();
	    newList.add(i);
	    combinations.add(newList);
	}
	index++;
	while(index < lists.size()) {
	    List<T> nextList = lists.get(index);
	    newCombinations = new HashSet<List<T>>();
	    for(List<T> first: combinations) {
		for(T second: nextList) {
		    List<T> newList = new ArrayList<T>();
		    newList.addAll(first);
		    newList.add(second);
		    newCombinations.add(newList);
		}
	    }
	    combinations = newCombinations;
	    
	    index++;
	}
	
	return combinations;
    }
    
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

	}catch(Exception e){e.printStackTrace();}
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
	}catch (Exception e){e.printStackTrace();}
    }
}
