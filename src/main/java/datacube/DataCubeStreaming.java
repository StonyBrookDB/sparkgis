package datacube;

import java.io.*;
import java.net.*;


/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
/* Spark imports */
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
/* Spark Streaming */
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
/* Local imports */
import datacube.data.DCObject;
import datacube.data.DCDimension;
import datacube.io.DCMongoDBDataAccess;
/* SparkGIS core imports */
import sparkgis.SparkGIS;
import sparkgis.core.io.mongodb.MongoStream;
import org.apache.spark.SparkConf;

public class DataCubeStreaming extends DataCube {

// variables to control streaming datacube output
public boolean started = false;
public int emptyCount = 0;
public long objectsRead = 0;
public long streamStart;

private JavaStreamingContext jsc;

public long objectsCount;

public int port_num =12358;
/**
 *
 */
public void buildStreaming(LinkedHashMap<String, Object> params){

        System.out.println("1111");

        // TEST ONLY: Don't include time in profiling
        objectsCount = sparkgis.core.io.mongodb.Mongo.getObjectsCount(params, null);

        if (dimensions.size() == 0)
                throw new RuntimeException("[DataCubeStreaming] No dimension specified");

        // Ignore this. This just logs datacube properties to console and file
        logDataCubeProperties("DataCube Stream", objectsCount);
        /*
         * Issue query to mongoDB to get min/max for all dimensions.
         * Calculated only once for a set of dimensions. Subsequent calls get saved values
         * Potential Bug: In case some data is updated in MongoDB
         */

        System.out.println("2222");
        logNgetMongoMinMax(params);

        // configure input
        // spgis = new SparkGIS(mongoIn, mongoIn);
        // jsc = new JavaStreamingContext(SparkGIS.sc, Durations.seconds(5)); //Durations.minutes(1));

        // SparkConf conf = new SparkConf().setAppName("Spark-GIS");
        // // .setMaster("local[2]")
        // // set serializer
        // // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");

        conf.set("spark.streaming.stopGracefullyOnShutdown","true");

        jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        System.out.println("33333");
        JavaReceiverInputDStream<DCObject> stream =
                jsc.receiverStream(new MongoStream(params, DCObject.class.getName()));



        JavaPairDStream<Integer, String> mappedValues =
                stream.mapToPair(new DCObjectMap(dimensions));

        mappedValues.foreachRDD(new SaveStream());

        //stream.print();

        System.out.println("44444");

        // Runtime.getRuntime().addShutdownHook(new Thread() {
        //  @Override
        //  public void run() {
        //      System.out.println("22Shutting down streaming app...");
        //
        //      try{
        //    jsc.stop(true, true);
        //   }
        //   catch(Exception e)
        //   {
        //        System.out.println("33333333333");
        //
        //   }
        //
        //
        //      System.out.println("Shutdown of streaming app complete.");
        //  }
        //     });



        jsc.start();

        System.out.println("555555");
        // jsc.awaitTermination( );
        // while(true)
        // {
        //   System.out.println("wait");
        // }


//jsc.awaitTerminationOrTimeout(5000);
        // System.out.println("666666");
        // timeout 12 seconds

        try{
                ServerSocket ss=new ServerSocket(port_num);
                Socket s=ss.accept(); //establishes connection
                DataInputStream dis=new DataInputStream(s.getInputStream());

                while(true)
                {

                        String str=(String)dis.readUTF();
                        System.out.println("message= "+str);
                        ss.close();
                        System.out.println("123 ");
                        break;
                }
                System.out.println("2456 ");
                jsc.stop(true, true);
                System.out.println("aaaa ");
        }
        catch(Exception e) {
                System.out.println("BUG1122");
                System.out.println(e);
        }



}

public void stopStream(){
        System.out.println("stopStream");
        // return;
        try{
                jsc.stop(true, true);
                // Thread.currentThread().interrupt();
        }
        catch(Exception e)
        {
                System.out.println("abcdefg");
        }

        //

        // jsc.stop();
        // SparkGIS.sc.stop();

        // if (mappedValues.partitions.isEmpty()){
        //     mappedValues.dstream().saveAsTextFiles(this.savePath + "/stream", "part");
        // }
        // System.out.println("Stream Count: " + stream.count());
        // System.out.println("Accum Value: " + MongoStream.objectsRead.value());

        //        try{
        // Thread.sleep(3000);}
        // catch(Exception e)
        // {
        //   System.out.println(e);
        // }
        // // jsc.stop();

//  try{
// jsc.stop(true, true);
// }
// catch(Exception e)
// {
//    System.out.println("2222222222222");
// }

        //        // SparkGIS.sc.stop();

}

class SaveStream
implements Function<JavaPairRDD<Integer, String>, Void>
{

public SaveStream
        (){

}

public Void call(JavaPairRDD<Integer, String> rdd){

        System.out.println("save_stream_result");


        if (!rdd.isEmpty()) {
                System.out.println("not empty,saving");
                if (!started) {
                        System.out.println("saving111");
                        streamStart = System.nanoTime();
                        started = true;
                }

                System.out.println("saving222");
                long count = rdd.count();
                objectsRead += count;

                System.out.println("saving333");
                System.out.println("RDD Count: " + objectsRead);

                // update datacube either in MongoDB or in HDFS
                System.out.println("DataCube.savePath4444\n\n\n");
                System.out.println(DataCube.savePath + "/stream");
                rdd.saveAsTextFile(DataCube.savePath + "/stream");

                // JUST FOR PROFILLING
                if (objectsRead == objectsCount) {


                        System.out.println("equal??\n\n\n");
                        // stopStream();


                        try{
                                Socket s=new Socket("localhost",port_num);
                                DataOutputStream dout=new DataOutputStream(s.getOutputStream());
                                dout.writeUTF("shut up streaming");
                                dout.flush();
                                // dout.close();
                                // s.close();
                        }catch(Exception e) {System.out.println(e); }










                        System.out.println("emptyCount:  "+emptyCount);
                        emptyCount++;
                        DataCube.profile(streamStart, "Total Streaming Time: ");
                        profile(-1, "---------------------------------------");
                }
        }
        else{

                System.out.println("save_stream_result_empty");
                if (started)
                        emptyCount++;
        }

        // if (started && emptyCount >= 1) {
        //         System.out.println("stop!!");
        //         // stopStream();
        // }
        return null;
}
}
}
