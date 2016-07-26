package sparkgis;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.lang.reflect.Field;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
/* Spark imports */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
/* Local imports*/
import sparkgis.pia.HMType;
import sparkgis.SparkGISConfig;
import sparkgis.task.HeatMapTask;
import sparkgis.core.io.ISparkGISIO;
import sparkgis.core.enums.Delimiter;
import sparkgis.core.enums.Predicate;
import sparkgis.task.SpatialJoinTask;

public class SparkGIS
{
public static final char TAB = '\t';

public static JavaSparkContext sc;
private final int threadCount = 8;

public final ISparkGISIO inputSrc;
public final ISparkGISIO outDest;

public SparkGIS(ISparkGISIO inputSrc, ISparkGISIO out){
        this.inputSrc = inputSrc;
        this.outDest = out;
        // Initialize JavaSparkContext
        SparkConf conf = new SparkConf().setAppName("Spark-GIS");
        // .setMaster("local[2]")
        // set serializer
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("textinputformat.record.delimiter", "\n");
        conf.set("spark.kryo.registrator", KryoClassRegistrator.class.getName());
        sc = new JavaSparkContext(conf);
        SparkGIS.Debug("Default parallelism: " + sc.defaultParallelism());
}

public static JavaSparkContext getContext() {
        return sc;
}
public void stop() {
        sc.stop();
}


/**
 * Added on April 29th, 2016
 * Dynamically load native libraries
 * Native libraries are bundled in jar and should be in 'lib/' folder
 * Add this folder to classpath
 */
private void setEnvironment(){
        System.setProperty( "java.library.path", SparkGISConfig.jarPath + "/lib/" );
        /*
         * Hack:
         * Java library path is check before application starts therefore it cannot be modified
         * However, if 'sys_paths' field of class loader is set to 'null', jvm is forced to reload it on any dependent library call
         */
        try{
                Field fieldSysPath = ClassLoader.class.getDeclaredField( "sys_paths" );
                fieldSysPath.setAccessible( true );
                fieldSysPath.set( null, null );
        }catch(Exception e) {e.printStackTrace(); }
}

public void spatialJoin(String dataPath1, String dataPath2, Delimiter delim, Predicate pred, int pSize, String outPath){
        SpatialJoinTask t = new SpatialJoinTask(inputSrc, dataPath1, dataPath2, delim, pred, outDest);
        // set optional parameters
        t.setPartitionSize(pSize);
        t.setOutPath(outPath);
        t.call();
}

public void heatMaps(List<String> algos, List<String> caseIDs, Predicate pred, HMType hmType, int pSize, String result_analysis_exe_id){

        // create a thread pool for async jobs
        ExecutorService exeService = Executors.newFixedThreadPool(threadCount);

        // for a given algorithm pair create parallel heatmap generation tasks
        List<HeatMapTask> tasks = new ArrayList<HeatMapTask>();
        for (String caseID : caseIDs) {
                HeatMapTask t = new HeatMapTask(inputSrc, caseID, algos, pred, hmType, outDest, result_analysis_exe_id);
                // set optional parameters
                t.setPartitionSize(pSize);
                tasks.add(t);
        }
        // wait for all jobs to complete
        try {
                List<Future<String> > results = exeService.invokeAll(tasks);
                for (Future res : results)
                        res.get();
                //System.out.println(res.get());
        }catch(Exception e) {e.printStackTrace(); }

        //close thread pool
        exeService.shutdown();

}

public static < E > String createTSString(E...args){
        String tss = "";
        for (E arg : args) {
                if (tss == "")
                        tss = tss + arg;
                else
                        tss = tss + TAB + arg;
        }
        //tss = tss + "\n";
        return tss;
}

public static < E > void Debug(E...vals){
        System.out.println("");
        System.out.print("[DEBUG] ");
        for (E val : vals) {
                System.out.print(val + " ");
        }
        System.out.println("");
}
}
