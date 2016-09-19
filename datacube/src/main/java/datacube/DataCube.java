package datacube;
/* Java imports */
import java.util.List;
<<<<<<< HEAD
import java.util.Set;
=======
>>>>>>> e2309c874cc88cac1cf2060fe64c898fdefb3ad7
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
<<<<<<< HEAD
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
/* Spark Streaming */
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
/* Local imports */
import datacube.task.DataLoadTask;
import datacube.data.DCObject;
//import datacube.data.Bucket;
import datacube.data.DCDimension;
import datacube.data.Property;
import datacube.data.DoubleProperty;
import datacube.data.PropertyName;
//import datacube.data.BucketDimension;

//import datacube.io.streaming.MongoStream;
import sparkgis.core.io.mongodb.MongoStream;

/* SparkGIS core imports */
import sparkgis.SparkGIS;
import sparkgis.core.io.ISparkGISIO;
//import sparkgis.core.io.mongodb.MongoDBDataAccess;
import datacube.io.DCMongoDBDataAccess;

public class DataCube implements Serializable{

    private final boolean DEBUG = true;
    // profile
    private final String jobTime;
    private String logFile = "datacube-stream-8-dim.log";

    private final String savePath;
    private DCMongoDBDataAccess mongoIn;

    private JavaRDD<DCObject> data = null;

    private List<DCDimension> dimensions;
    
    public DataCube(){
	dimensions = new ArrayList<DCDimension>();

	Calendar cal = Calendar.getInstance();
	SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy-HH-mm");
	jobTime = sdf.format(cal.getTime());

	savePath = "hdfs://10.10.10.11/user/fbaig/datacube/datacube-"+jobTime;
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

	    String outFileName = "logs/datacube/"+this.logFile;
	    
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

    public void buildCube(LinkedHashMap<String, Object> params){
    //public void buildCube(LinkedHashMap<String, String> params){

	// basic error checking
	if (dimensions.size() == 0)
	    throw new RuntimeException("[DataCube] No dimension specified");
	
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
	if (data == null){
	    data = load((long)10000, params);
	    //return;
	}
	
	profile(-1, "Total # of objects: " + data.count());
	profile(-1, "---------------------------------------");
	profile(-1, "HDFS save path: " + this.savePath);
	profile(-1, "---------------------------------------");
	profile(loadStart, "Data Load Time: ");

	long start = System.nanoTime();
	mongoIn.getMinMax(dimensions, 0, params);
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
	    	});
	    //.sortByKey();

	mappedValues.count();
	profile(dcBuildStart, "Map Values to Buckets: ");

	long hdfsOut = System.nanoTime();

	// for DEBUGGING
	mappedValues.saveAsTextFile(this.savePath);

	profile(hdfsOut, "HDFS Out: ");
	
	// List<Integer> mappedKeys =  mappedValues.groupByKey().keys().collect();
	// System.out.println("Mapped Count: " + mappedValues.count())
	
	//System.out.println("Not Empty Buckets: " + mappedKeys.size());
	//System.out.println("Sparsity Measure: " + (double)mappedKeys.size()/buckets.size());

	SparkGIS.sc.stop();
    }

    
    /**
     * 
     */
    public void buildStreaming(LinkedHashMap<String, Object> params){
	//mongoIn = new DCMongoDBDataAccess();
	
	// basic error checking
	if (dimensions.size() == 0)
	    throw new RuntimeException("[DataCube] No dimension specified");

	profile(-1, "HDFS save path: " + this.savePath);

	profile(-1, "MongoDB min/max ...");
	mongoIn = new DCMongoDBDataAccess();
	mongoIn.getMinMax(dimensions, 0, params);
	
	for (DCDimension dim : dimensions)
	    System.out.println(dim.getNameStr() + ": Min:" + dim.getMin() + ", Max: " + dim.getMax());

	// configure input
    	final SparkGIS spgis = new SparkGIS(mongoIn, mongoIn);
	
	JavaStreamingContext jsc = new JavaStreamingContext(SparkGIS.sc, Durations.seconds(10));//Durations.minutes(1));

	JavaReceiverInputDStream<DCObject> stream = 
	    jsc.receiverStream(new MongoStream(params, DCObject.class.getName()));

	JavaPairDStream<Integer, String> mappedValues =
	    stream.mapToPair(new DCObjectMap());

	mappedValues.dstream().saveAsTextFiles(this.savePath + "/stream", "part");
	
	//System.out.println("Stream Count: " + stream.count());
	//System.out.println("Accum Value: " + MongoStream.objectsRead.value());
	
	stream.print();

	jsc.start();
	jsc.awaitTermination();
	// timeout 12 seconds
	//jsc.awaitTerminationOrTimeout(50000);
	jsc.stop(true, true);
	//SparkGIS.sc.stop();
    }

    // make this private. public just for testing
    public JavaRDD<DCObject> load(Long loadBatchSize, LinkedHashMap<String, Object> params){
    //private JavaRDD<DCObject> load(Long loadBatchSize, LinkedHashMap<String, String> params){
	mongoIn = new DCMongoDBDataAccess();
    	// configure input
    	SparkGIS spgis = new SparkGIS(mongoIn, mongoIn);
	

	//JavaPairRDD<>
	//System.out.println("MongoIn Count: " + mongoIn.getDataRDDMongoHadoop(params).count());

	
	//return null;

	System.out.println("Before getDataRDD ...");
	
    	// keep all data in memory as a central data referencing system
    	// e.g. like star/snow flake schema
	JavaRDD<DCObject> objRDD = mongoIn.getDataRDD(params, DCObject.class.getName()).cache();
	
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
=======
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
/* Local imports */
import datacube.data.DCObject;
import datacube.data.PropertyName;
import datacube.data.DCDimension;
/* SparkGIS core imports */
import sparkgis.SparkGIS;
import datacube.io.DCMongoDBDataAccess;

public class DataCube implements Serializable {

protected static SparkGIS spgis;

public static final boolean DEBUG = true;
// profile
private String jobTime;
private static String logFile = "seq.log";

public static String savePath;
protected DCMongoDBDataAccess mongoIn;

// all data
private JavaRDD<DCObject> data = null;

protected List<DCDimension> dimensions;

public DataCube(){
        dimensions = new ArrayList<DCDimension>();

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy-HH-mm");
        jobTime = sdf.format(cal.getTime());

        // DataCube.savePath = "hdfs://10.10.10.11/user/fbaig/datacube/datacube-"+jobTime;


        DataCube.savePath = "/home/cochung/datacube_results/datacube-"+jobTime;


}

public void addDimension(PropertyName name, int bucketCount){
        dimensions.add(new DCDimension(name, bucketCount));
}

public static void profile(long start, String desc){
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
        // try {

        //         String outFileName = "logs/datacube/stream-vs-seq/"+DataCube.logFile;

        //         PrintWriter out =
        //                 new PrintWriter(new BufferedWriter(new FileWriter(outFileName, true)));
        //         System.out.println(str);
        //         out.println(str);
        //         out.close();
        // }catch(Exception e) {e.printStackTrace(); }
}

private void indexToHumanReadable(int...indices){
        if (indices.length != dimensions.size())
                throw new RuntimeException("[DataCube] Invalid indices");
        // for (int i=0; i<indices.length; ++i){
        //     DCDimension dim = dimensions.get(i);
        //     System.out.println(dim.getName());
        //     int count = 0;
        //     for (double j=dim.getMin(); j<dim.getMax(); j+=dim.getResolution()){
        //  count++;
        //  if (count == indices[i]){
        //      System.out.println(j+"<= "+dim.getName()+ "<" +(j+dim.getResolution()));
        //  }
        //     }
        // }
}

public void buildCube(LinkedHashMap<String, Object> params){
        //public void buildCube(LinkedHashMap<String, String> params){

        // basic error checking
        if (dimensions.size() == 0)
                throw new RuntimeException("[DataCube] No dimension specified");

        // load all data in memory
        long loadStart = System.nanoTime();
        if (data == null) {
                data = load((long)10000, params);
        }


        List<DCObject> datalist = data.collect();


        System.out.println("hey!!!!!");

        for (DCObject ts : datalist)
                System.out.println(ts);


        // for(int i=0; i< 10;i++)
        // {
        //   System.out.println(i+":  "+datalist.get[2]);
        // }


        // Ignore this. This just logs datacube properties to console and file
        logDataCubeProperties("DataCube", data.count());

        profile(loadStart, "Data Load Time: ");

        /*
         * Issue query to mongoDB to get min/max for all dimensions.
         * Calculated only once for a set of dimensions. Subsequent calls get saved values
         * Potential Bug: In case some data is updated in MongoDB
         */
        logNgetMongoMinMax(params);

        long dcBuildStart = System.nanoTime();

        // // broadcast dimensions details
        // //dims = SparkGIS.sc.broadcast(dimensions);

        // JavaPairRDD<Integer, String> mappedValues =
        //         data.mapToPair(new DCObjectMap(dimensions))
        //         .filter(new Function<Tuple2<Integer, String>, Boolean>(){
        //                         public Boolean call(Tuple2<Integer, String> t){
        //                                 return (t==null) ? false : true;
        //                         }
        //                 });

         JavaPairRDD<String, String> mappedValues =
                data.mapToPair(new DCObjectMap(dimensions))
                .filter(new Function<Tuple2<String, String>, Boolean>(){
                                public Boolean call(Tuple2<String, String> t){
                                        return (t==null) ? false : true;
                                }
                        });
        //.sortByKey();

        mappedValues.count();
        profile(dcBuildStart, "Map Values to Buckets: ");

        long hdfsOut = System.nanoTime();

        // for DEBUGGING
        mappedValues.saveAsTextFile(this.savePath);

        profile(hdfsOut, "HDFS Out: ");

        profile(loadStart, "Total Time: ");

        // List<Integer> mappedKeys =  mappedValues.groupByKey().keys().collect();
        // System.out.println("Mapped Count: " + mappedValues.count())

        //System.out.println("Not Empty Buckets: " + mappedKeys.size());
        //System.out.println("Sparsity Measure: " + (double)mappedKeys.size()/buckets.size());

        SparkGIS.sc.stop();
}

// make this private. public just for testing
public JavaRDD<DCObject> load(Long loadBatchSize, LinkedHashMap<String, Object> params){
        mongoIn = new DCMongoDBDataAccess();
        // configure input
        spgis = new SparkGIS(mongoIn, mongoIn);

        System.out.println("Before getDataRDD ...");

        // keep all data in memory as a central data referencing system
        // e.g. like star/snow flake schema
        JavaRDD<DCObject> objRDD = mongoIn.getDataRDD(params, DCObject.class.getName()).cache();

        System.out.println("Count: " + objRDD.count());
        return objRDD;
}

protected void logNgetMongoMinMax(LinkedHashMap<String, Object> params){
        long start = System.nanoTime();
        mongoIn = new DCMongoDBDataAccess();
        mongoIn.getMinMax(dimensions, 0, params);
        profile(start, "MongoDB min/maxTime: ");

        for (DCDimension dim : dimensions)
                System.out.println(dim.getNameStr() + ": Min:" + dim.getMin() + ", Max: " + dim.getMax());

        //inMemoryMinMax();
}

protected void logDataCubeProperties(String name, long count){
        profile(-1, "***************************************\n" + name);
        profile(-1, "Dimensions (Name, # of Buckets)");
        profile(-1, "---------------------------------------");
        String dimStr = "";
        for (DCDimension dim : dimensions) {
                dimStr += dim.getNameStr() + ", ";
                dimStr += dim.getBucketCount() + ", ";
                //dimStr += dim.getMin() + ", " + dim.getMax();
                dimStr += "\n";
        }
        profile(-1, dimStr);
        profile(-1, "---------------------------------------");

        profile(-1, "Total # of objects: " + count);
        profile(-1, "---------------------------------------");
        profile(-1, "HDFS save path: " + this.savePath);
        profile(-1, "---------------------------------------");
}

/********************* IN MEMORY Min/Max *****************************/

// private void inMemoryMinMax(){
//  List<PropertyName> names = new ArrayList<PropertyName>();
//  for (DCDimension dim:dimensions)
//      names.add(dim.getName());

//  long minMaxStart = System.nanoTime();

//  System.out.println("Mins:");
//  List<DoubleProperty> dimensionsMin = getMinMax(names, true);
//  System.out.println("Maxs:");
//  List<DoubleProperty> dimensionsMax = getMinMax(names, false);


//  // can be improved (SHOULD BE!!!)
//  mapMinMax(dimensionsMin, dimensionsMax);

//  profile(minMaxStart, "MinMax Time: ");
// }

// private void foo(DCDimension dim, List<DoubleProperty> dimensionsMin, List<DoubleProperty>dimensionsMax){
//  for (DoubleProperty dp:dimensionsMin){
//      if (dp.getName().equals(dim.getName()))
//    dim.setMin(dp.getValue());
//  }
//  for (DoubleProperty dp:dimensionsMax){
//      if (dp.getName().equals(dim.getName()))
//    dim.setMax(dp.getValue());
//  }
// }

// private void mapMinMax(List<DoubleProperty> mins, List<DoubleProperty>maxs){
//  for (DCDimension dim:dimensions){
//      foo(dim, mins, maxs);
//  }
// }

// /**
//  * @param min True: returns minimums, False: retrun maximums
//  */
// private List<DoubleProperty> getMinMax(final List<PropertyName> propNames, final boolean min){

//  List<DoubleProperty> mins =
//      data.map(new Function<DCObject, List<DoubleProperty>>(){

//        private DoubleProperty getPropValue(DCObject obj, final PropertyName propName){
//      final Property prop = new Property(propName, null);
//      for (Property p:obj.props){
//          if (p.equals(prop))
//        return new DoubleProperty(propName, new Double(p.getValue().toString()));
//      }
//      return null;
//        }

//        public List<DoubleProperty> call(DCObject obj){
//      List<DoubleProperty> ret = new ArrayList<DoubleProperty>();
//      //for (DCDimension dim:curr_dims){
//      for (PropertyName pn:propNames){
//          DoubleProperty dp = getPropValue(obj, pn);
//          if (dp != null)
//        ret.add(dp);
//      }

//      return ret;
//        }
//    }).filter(new Function<List<DoubleProperty>, Boolean>(){
//      public Boolean call(List<DoubleProperty> d){return !d.isEmpty();}
//        }).reduce(new Function2<List<DoubleProperty>, List<DoubleProperty>, List<DoubleProperty>>(){
//          public List<DoubleProperty> call(List<DoubleProperty> d1, List<DoubleProperty> d2){
//        List<DoubleProperty> ret = new ArrayList<DoubleProperty>();
//        for (int i=0; i<d1.size() && i<d2.size(); ++i){
//            DoubleProperty dp1 = d1.get(i);
//            DoubleProperty dp2 = d2.get(i);
//            if (min){
//          if (dp1.getValue()<dp2.getValue())
//              ret.add(new DoubleProperty(dp1.getName(), dp1.getValue()));
//          else
//              ret.add(new DoubleProperty(dp2.getName(), dp2.getValue()));
//            }
//            else {
//          if (dp1.getValue()>dp2.getValue())
//              ret.add(new DoubleProperty(dp1.getName(), dp1.getValue()));
//          else
//              ret.add(new DoubleProperty(dp2.getName(), dp2.getValue()));
//            }
//        }
//        return ret;
//          }
//      });

//  for (DoubleProperty dp:mins)
//      System.out.println(dp.getNameStr() + ":" + dp.getValue());
//  return mins;//0.0;
// }
>>>>>>> e2309c874cc88cac1cf2060fe64c898fdefb3ad7

}