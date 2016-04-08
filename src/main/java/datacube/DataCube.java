package datacube;
/* Java imports */
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.io.Serializable;
import java.util.LinkedHashMap;
// for profile
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
/* Local imports */
import datacube.task.DataLoadTask;
import datacube.data.DCObject;
//import datacube.data.Bucket;
import datacube.data.DCDimension;
import datacube.data.Property;
import datacube.data.DoubleProperty;
import datacube.data.PropertyName;
//import datacube.data.BucketDimension;
/* SparkGIS core imports */
import sparkgis.SparkGIS;
import sparkgis.core.io.ISparkGISIO;
//import sparkgis.core.io.mongodb.MongoDBDataAccess;
import datacube.io.DCMongoDBDataAccess;

public class DataCube implements Serializable{

    private final boolean DEBUG = true;
    
    // Test values
    
    // {caseid} = ~ 
    private final String db = "u24_nodejs";
    private final String collection = "objects";
    private final String analysis_exe_id = "yi-algo-v2";
    private final String caseID1 = "TCGA-CS-4941-01Z-00-DX1"; // ~65191
    private final String caseID2 = "TCGA-CS-5393-01Z-00-DX1"; // ~472518
    
    DCMongoDBDataAccess mongoIn;
    LinkedHashMap<String, Object> params;
    
    private JavaRDD<DCObject> data = null;

    //Broadcast<List<DCDimension>> dims;
    //private List<Bucket> buckets;

    private List<DCDimension> dimensions;

    // profile
    private final String jobTime;
    
    public DataCube(){
	dimensions = new ArrayList<DCDimension>();
	//buckets = new ArrayList<Bucket>();

	Calendar cal = Calendar.getInstance();
	SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy-HH-mm");
	jobTime = sdf.format(cal.getTime());
    }
    
    public void addDimension(PropertyName name, int bucketCount){
	dimensions.add(new DCDimension(name, bucketCount));
    }
    
    private void profile(long start, String desc){
	String str = null;
	if (start == -1)
	    str = desc;
	else{
	    long exeTime = System.nanoTime() - start;
	    long seconds = TimeUnit.SECONDS.convert(exeTime, TimeUnit.NANOSECONDS);
	    if (seconds > 0)
		str = desc + seconds + " s";
	    else{
		long ms = TimeUnit.MILLISECONDS.convert(exeTime, TimeUnit.NANOSECONDS);
		str = desc + "0 s (" + ms + "ms)";
	    }
	}
	try {

	    String outFileName = "logs/datacube/datacube.log";
	    
    	    PrintWriter out = 
    		new PrintWriter(new BufferedWriter(new FileWriter(outFileName, true)));
	    System.out.println(str);
	    out.println(str);
	    //out.println("*****************************************************");
	    out.close();
	}catch(Exception e){e.printStackTrace();}
    }

    private void indexToHumanReadable(int... indices){
	if (indices.length != dimensions.size())
	    throw new RuntimeException("[DataCube] Invalid indices");
	// for (int i=0; i<indices.length; ++i){
	//     DCDimension dim = dimensions.get(i);
	//     System.out.println(dim.getName());
	//     int count = 0;
	//     for (double j=dim.getMin(); j<dim.getMax(); j+=dim.getResolution()){
	// 	count++;
	// 	if (count == indices[i]){
	// 	    System.out.println(j+"<= "+dim.getName()+ "<" +(j+dim.getResolution()));
	// 	}
	//     }
	// }
    }
    
    public void storeCube(){
	// dimensions
	// each dimensions' resolution
	// each dimensions' min/max
    }
    
    public void buildCube(){

	profile(-1, "***************************************\nDataCube");
	profile(-1, "Dimensions (Name, # of Buckets)");
	profile(-1, "---------------------------------------");
	String dimStr = "";
	for (DCDimension dim : dimensions){
	    dimStr += dim.getNameStr() + ", ";
	    dimStr += dim.getBucketCount() + ", ";
	    //dimStr += dim.getMin() + ", " + dim.getMax();
	    dimStr += "\n";
	}
	profile(-1, dimStr);
	profile(-1, "---------------------------------------");
		
	// load all data in memory
	// FIX: Not suitable for large amount of data
	//      Add streaming data load capability

	long loadStart = System.nanoTime();
	if (data == null)
	    data = load((long)10000);

	profile(-1, "Total # of objects: " + data.count());
	profile(-1, "---------------------------------------");
	profile(loadStart, "Data Load Time: ");

	long start = System.nanoTime();
	mongoIn.getMinMax(dimensions, db, collection, 0, params);
	profile(start, "MongoDB min/maxTime: ");

	for (DCDimension dim : dimensions)
	    System.out.println(dim.getNameStr() + ": Min:" + dim.getMin() + ", Max: " + dim.getMax());
	//inMemoryMinMax();
	
	
	long dcBuildStart = System.nanoTime();

	// // broadcast dimensions details
	// //dims = SparkGIS.sc.broadcast(dimensions);

	JavaPairRDD<Integer, String> mappedValues = 
	    data.mapToPair(new DCObjectMap())
	    .filter(new Function<Tuple2<Integer, String>, Boolean>(){
		    public Boolean call(Tuple2<Integer, String> t){return (t==null)?false:true;}
		})
	    .sortByKey();

	// for DEBUGGING
	mappedValues.saveAsTextFile("hdfs://10.10.10.11/user/fbaig/datacube/datacube-"+jobTime);
	
	// List<Integer> mappedKeys =  mappedValues.groupByKey().keys().collect();
	// System.out.println("Mapped Count: " + mappedValues.count());
	
	profile(dcBuildStart, "Map Values to Buckets: ");
	
	//System.out.println("Not Empty Buckets: " + mappedKeys.size());
	//System.out.println("Sparsity Measure: " + (double)mappedKeys.size()/buckets.size());
    }

    private JavaRDD<DCObject> load(Long loadBatchSize){
	mongoIn = new DCMongoDBDataAccess();
    	// configure input
    	final SparkGIS spgis = new SparkGIS(mongoIn, mongoIn);
	
    	params = new LinkedHashMap<String, Object>();
    	params.put("db", db);
    	params.put("collection", collection);
    	params.put("provenance.analysis_execution_id", analysis_exe_id);
	
	// List<String> orClauses = new ArrayList<String>();
	// orClauses.add(caseID1);
	// orClauses.add(caseID2);

	// LinkedHashMap<String, List> or = new LinkedHashMap<String, List>();
	// or.put("image.caseid", orClauses);
	// params.put("or", or);
	
	params.put("image.caseid", caseID2);
	
    	// keep all data in memory as a central data referencing system
    	// e.g. like star/snow flake schema
	JavaRDD<DCObject> objRDD = mongoIn.getDataRDD(params).cache();
	
	// Streaming load
	//JavaRDD<DCObject> objRDD = mongoIn.getDataRDD(params, (long)0).cache();
	// Long totalObjects = mongoIn.getObjectsCount(params, null);
	// List<DataLoadTask> tasks = new ArrayList<DataLoadTask>();
	// for (long i=0; i<totalObjects; i+=loadBatchSize){
	//     tasks.add(new DataLoadTask(i, mongoIn, params));
	// }

    	System.out.println("Count: " + objRDD.count());
	
    	return objRDD;
    }
        
    /**
     * Calculate appropriate datacube bucket-id for each object
     * @return 'DataCube Bucket-ID', 'Object-ID'
     */
    class DCObjectMap implements PairFunction<DCObject, Integer, String>{

	private int getMultiplier(int index){
	    int multiplier = 1;
	    for (int i=dimensions.size()-1; i>index; --i){
		multiplier *= dimensions.get(i).getBucketCount();
	    }
	    return multiplier;
	}
	
	/**
	 * Area 0-100, resolution: 10, buckets = 10 (i=0: 0<=Area<10, i=1: 10<=Area<20 ...)
	 * ELongation 0-2, resolution: 0.1, buckets = 20 (j=0: 0<=Elongation<0.1, j=1: 0.1<=ELongation<0.2 ...)
	 */
	private int mapIndex(int...  indices){
	    if (indices.length != dimensions.size())
		throw new RuntimeException("[DataCube] Invalid indices");
	    // change to more than int
	    int linearIndex = 0;
	    int i;
	    for (i=0; i<(indices.length-1); ++i){
		if (indices[i] > (dimensions.get(i).getBucketCount()-1)){
		    String str = "[DataCube] Index out of bound, index: "+
			indices[i]+", max: "+(dimensions.get(i).getBucketCount()-1);
		    throw new RuntimeException(str);
		}
		
		linearIndex += indices[i] * getMultiplier(i);
	    }
	    if (indices[i] > (dimensions.get(i).getBucketCount()-1)){
		String str = "[DataCube] Index out of bound, index: "+indices[i]+", max: "+(dimensions.get(i).getBucketCount()-1);
		throw new RuntimeException(str);
	    }
	     	
	    linearIndex += indices[i];
	    return linearIndex;
	}
	
	private Double getValue(PropertyName prop, DCObject obj){
	    for (Property p : obj.props){
		if ((p instanceof DoubleProperty) && (p.getNameStr().equals(prop.value))){
		    return ((DoubleProperty)p).getValue();
		}
	    }
	    return null;
	}

	private int getIndex(Double value, DCDimension dim){
	    // do it using max ...
	    int index = 0;
	    final Double resolution = dim.getResolution();
	    // (resolution < 1) -> larger index value
	    //if (resolution >= 1){
		int ret = (int)(value/resolution);
		// croner case: (value = max) -> (ret = bucketCount)
		return (ret >= dim.getBucketCount()) ? dim.getBucketCount()-1 : ret; 
		//}
	    // else{
	    // 	for (double i=dim.getMin(); i<dim.getMax() ; i+=dim.getResolution()){
	    // 	    if ((value >= i) && (value < (i+dim.getResolution())) )
	    // 		return index;
	    // 	    index++;
	    // 	}
	    // }
	    // return (index-1);
	}
	
    	public Tuple2<Integer, String> call(DCObject obj){

	    String ret = obj.getID();
	    
	    int[] indices = new int[dimensions.size()];
	    int i=0;
	    for (DCDimension dim:dimensions){
		Double curr_value = getValue(dim.getName(), obj);
		int index = getIndex(curr_value, dim);
	
		if (index > (dim.getBucketCount()-1) || (index < 0)){
		    throw new RuntimeException("[DataCube] Index out of bound"+
					       ", index: "+ index +
					       ", max: "+ (dimensions.get(i).getBucketCount()-1)+
					       ", value: " + curr_value + 
					       ", name: " + dim.getNameStr()
					       );
		}
		indices[i++] = index;
		
		if (DEBUG){
		    // ret += "\t" + dim.getName().value + ":" + curr_value + "\tMin:"+dim.getMin() + "\tMax:" + dim.getMax() + "\tResolution:" + dim.getResolution() + "\t";
		    ret += "\t" + dim.getName().value + ":" + curr_value + "\t";
		}
	    }

	    if (DEBUG){
		ret += "(";
		for (int ind:indices)
		    ret += ind + ",";
		ret += ")";
	    }
	    
	    // map indices
	    int lIndex = mapIndex(indices);
	    
	    return new Tuple2<Integer, String>(lIndex, ret);
    	}
    }
    
    
    /********************* IN MEMORY Min/Max *****************************/

    // private void inMemoryMinMax(){
    // 	List<PropertyName> names = new ArrayList<PropertyName>();
    // 	for (DCDimension dim:dimensions)
    // 	    names.add(dim.getName());
	
    // 	long minMaxStart = System.nanoTime();

    // 	System.out.println("Mins:");
    // 	List<DoubleProperty> dimensionsMin = getMinMax(names, true);
    // 	System.out.println("Maxs:");
    // 	List<DoubleProperty> dimensionsMax = getMinMax(names, false);
	
	
    // 	// can be improved (SHOULD BE!!!)
    // 	mapMinMax(dimensionsMin, dimensionsMax);
	
    // 	profile(minMaxStart, "MinMax Time: ");
    // }
    
    // private void foo(DCDimension dim, List<DoubleProperty> dimensionsMin, List<DoubleProperty>dimensionsMax){
    // 	for (DoubleProperty dp:dimensionsMin){
    // 	    if (dp.getName().equals(dim.getName()))
    // 		dim.setMin(dp.getValue());
    // 	}
    // 	for (DoubleProperty dp:dimensionsMax){
    // 	    if (dp.getName().equals(dim.getName()))
    // 		dim.setMax(dp.getValue());
    // 	}
    // }
    
    // private void mapMinMax(List<DoubleProperty> mins, List<DoubleProperty>maxs){
    // 	for (DCDimension dim:dimensions){
    // 	    foo(dim, mins, maxs);
    // 	}
    // }
    
    // /**
    //  * @param min True: returns minimums, False: retrun maximums
    //  */
    // private List<DoubleProperty> getMinMax(final List<PropertyName> propNames, final boolean min){

    // 	List<DoubleProperty> mins = 
    // 	    data.map(new Function<DCObject, List<DoubleProperty>>(){
		
    // 		    private DoubleProperty getPropValue(DCObject obj, final PropertyName propName){
    // 			final Property prop = new Property(propName, null);
    // 			for (Property p:obj.props){
    // 			    if (p.equals(prop))
    // 				return new DoubleProperty(propName, new Double(p.getValue().toString()));
    // 			}
    // 			return null;
    // 		    }
		    
    // 		    public List<DoubleProperty> call(DCObject obj){
    // 			List<DoubleProperty> ret = new ArrayList<DoubleProperty>();
    // 			//for (DCDimension dim:curr_dims){
    // 			for (PropertyName pn:propNames){
    // 			    DoubleProperty dp = getPropValue(obj, pn);
    // 			    if (dp != null)
    // 				ret.add(dp);
    // 			}
			
    // 			return ret;
    // 		    }
    // 		}).filter(new Function<List<DoubleProperty>, Boolean>(){
    // 			public Boolean call(List<DoubleProperty> d){return !d.isEmpty();}
    // 		    }).reduce(new Function2<List<DoubleProperty>, List<DoubleProperty>, List<DoubleProperty>>(){
    // 			    public List<DoubleProperty> call(List<DoubleProperty> d1, List<DoubleProperty> d2){ 
    // 				List<DoubleProperty> ret = new ArrayList<DoubleProperty>();
    // 				for (int i=0; i<d1.size() && i<d2.size(); ++i){
    // 				    DoubleProperty dp1 = d1.get(i);
    // 				    DoubleProperty dp2 = d2.get(i);
    // 				    if (min){
    // 					if (dp1.getValue()<dp2.getValue())
    // 					    ret.add(new DoubleProperty(dp1.getName(), dp1.getValue()));
    // 					else
    // 					    ret.add(new DoubleProperty(dp2.getName(), dp2.getValue()));
    // 				    }
    // 				    else {
    // 					if (dp1.getValue()>dp2.getValue())
    // 					    ret.add(new DoubleProperty(dp1.getName(), dp1.getValue()));
    // 					else
    // 					    ret.add(new DoubleProperty(dp2.getName(), dp2.getValue()));
    // 				    }
    // 				}
    // 				return ret;
    // 			    }
    // 			});
	
    // 	for (DoubleProperty dp:mins)
    // 	    System.out.println(dp.getNameStr() + ":" + dp.getValue());
    // 	return mins;//0.0; 
    // }

}
