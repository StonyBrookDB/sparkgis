package sparkgis.stats;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.TimeUnit;
// /* for asyncHeatMap */
import java.util.List;
import java.util.ArrayList;
import sparkgis.SparkGIS;
import java.util.Map;
import java.util.LinkedHashMap;
//import sparkgis.data.RawData;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
import sparkgis.enums.Predicate;
import sparkgis.enums.HMType;
import sparkgis.enums.IO;
import sparkgis.executionlayer.SparkPrepareData;

public class Profile
{
    public static Map<String, Long> log = new LinkedHashMap<String, Long>();

    public static void yank_log(){
	long first = 0;
	for (Map.Entry e : log.entrySet()){
	    System.out.print(e.getKey() + "\t");
	    Long val = (Long)e.getValue();
	    if (first == 0){
		first = val.longValue();
	    }
	    System.out.println(TimeUnit.MILLISECONDS.convert(val.longValue()-first, TimeUnit.NANOSECONDS) + "ms");
	    
	}
    }
    
    private static int processed = 0;
    private static int total = 0;

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

    private static ArrayList<Detail> details = new ArrayList<Detail>();
    public static void log(String field, long value){
    	details.add(new Detail(field, value));
    }

    public static void printProgress(){
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
}
