package datacube;

import java.io.*;
import java.net.*;

import scala.Tuple2;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
/* Spark imports */
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
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




import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


import java.util.Map;
import java.util.Iterator;


import java.util.ArrayList;
import java.util.*;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;



public class DataCubeStreaming extends DataCube {

// variables to control streaming datacube output


public Map<String, ArrayList<String> > savedMap = new HashMap<String, ArrayList<String> >();
public boolean started = false;
public int emptyCount = 0;
public long objectsRead = 0;
public long streamStart;

private JavaStreamingContext jsc;

public long objectsCount;

public ArrayList al;

public int port_num =43313;
/**
 *
 */
public void buildStreaming(LinkedHashMap<String, Object> params){

        al  = new ArrayList();

        System.out.println("1111");

        // TEST ONLY: Don't include time in profiling
        objectsCount = sparkgis.core.io.mongodb.Mongo.getObjectsCount(params, null);

        if (dimensions.size() == 0)
                throw new RuntimeException("[DataCubeStreaming] No dimension specified");

        // Ignore this. This just logs datacube properties to console and file
        logDataCubeProperties("DataCube Stream", objectsCount);
        /*
         * Issue query to dmongoDB to get min/max for all dimensions.
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


        DCObjectMap dcMap =   new DCObjectMap(dimensions);

        JavaPairDStream<String, String> mappedValues =
                stream.mapToPair(dcMap);

        mappedValues.foreachRDD(new SaveStream());

        // mappedValues.foreachRDD(rdd -> {
        //                                 rdd.foreachPartition(
        //                                         items -> {
        //                                                 while (items.hasNext()) {
        //                                                         System.out.println("items.next() + System.lineSeparator()");
        //                                                 }
        //                                         });
        //
        //                         });



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
        jsc.awaitTermination( );
        // while(true)
        // {
        //   System.out.println("wait");
        // }


        // jsc.awaitTerminationOrTimeout(500);
        // System.out.println("666666");
        // timeout 12 seconds

        // try{
        //         ServerSocket ss=new ServerSocket(port_num);
        //         Socket s=ss.accept(); //establishes connection
        //         DataInputStream dis=new DataInputStream(s.getInputStream());
        //
        //         while(true)
        //         {
        //
        //                 String str=(String)dis.readUTF();
        //                 System.out.println("message= "+str);
        //                 ss.close();
        //                 System.out.println("123 ");
        //                 break;
        //         }
        //         System.out.println("2456 ");
        //         jsc.stop(true, true);
        //         System.out.println("aaaa ");
        // }
        // catch(Exception e) {
        //         System.out.println("BUG1122");
        //         System.out.println(e);
        // }



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
implements Function<JavaPairRDD<String, String>, Void>
{

public SaveStream
        (){

}



public Void call(JavaPairRDD<String, String> rdd){

        System.out.println("save_stream_result");



        // if(!rdd.partitions().isEmpty()) {
        //         rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String> > >() {
        //                                      @Override
        //                                      public void call(Iterator<Tuple2<String,String> > tuple2Iterator) throws Exception {
        //
        //                                              // HBaseConnectionFactory.init();
        //                                              while (tuple2Iterator.hasNext()) {
        //                                                      System.out.println(  tuple2Iterator.next()._1);
        //                                                      // System.out.println("save_stream_result");
        //                                              }
        //                                      }
        //                              });
        // }


        // if(!rdd.partitions().isEmpty()) {
        //         rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String> > >() {
        //                                      @Override
        //                                      public void call(Iterator<Tuple2<String,String> > tuple2Iterator) throws Exception {
        //
        //                                              // HBaseConnectionFactory.init();
        //                                              while (tuple2Iterator.hasNext()) {
        //                                                      //  System.out.println(  tuple2Iterator.next()._1);
        //                                                      // System.out.println("save_stream_result");
        //                                              }
        //                                      }
        //                              });
        // }













        //  rdd.foreach(new VoidFunction<String>(){ public void call(String line) {
        //   if (line.contains("KK6JKQ")) {
        //     count.add(1);
        //   }
        // }});
        // if (!rdd.isEmpty()) {
        //         ArrayList<String> list = new ArrayList<String>();
        //         for (Tuple2<String, String> test : rdd.collect())  {
        //                 // System.out.println(test._1 );
        //                 // System.out.println(test._2);
        //
        //                 list.add(test._1);
        //
        //                 // String host = sparkgis.SparkGISConfig.mongoHost;
        //                 // int port = sparkgis.SparkGISConfig.mongoPort;
        //                 //
        //                 // MongoClient mongoClient = new MongoClient(host, port);
        //                 // DB db =  mongoClient.getDB("metadata_db");
        //                 //
        //                 // // System.out.println("helloworld!!!!!!!!!!!");
        //                 //
        //                 // // System.out.println(outputdbCollection);
        //                 //
        //                 // DBCollection results = db.getCollection("metadata_col");
        //                 //
        //                 // BasicDBObject wdoc = new BasicDBObject("analysis_execution_id", 0)
        //                 //                      .append("x", test._1)
        //                 //                      .append("tile_id", test._2);
        //                 // results.insert(wdoc);
        //                 // mongoClient.close();
        //
        //
        //         }
        //
        //         long count = list.size();
        //         objectsRead += count;
        //         System.out.println("size:    "+ objectsRead);
        //
        // }




        if (!rdd.isEmpty()) {
                System.out.println("not empty,saving");



                ArrayList<String> list = new ArrayList<String>();

                // try {
                //         FileWriter fw = new FileWriter("test._1", true);
                //         BufferedWriter bw = new BufferedWriter(fw);
                //         PrintWriter out = new PrintWriter(bw);
                //         // System.out.println("second:   "+test._2);
                //         out.println("test._2");
                //         out.close();
                //         //more code
                //         // out.println("more text");
                //         // //more code
                // } catch (IOException e) {
                //         //exception handling left as an exercise for the reader
                // }

                for (Tuple2<String, String> test : rdd.collect())  {
                        // System.out.println("first:   "+ test._1 );
                        // System.out.println("second:   "+test._2);

                        ArrayList<String> getList =  (  ArrayList<String> )(savedMap.get(test._1));
                        if(getList == null)
                        {
                                getList  =  new ArrayList<String>();
                                getList.add(test._2);
                                savedMap.put(test._1,getList);
                        }
                        getList.add(test._2);





                        // try {
                        //         FileWriter fw = new FileWriter(test._1, true);
                        //         BufferedWriter bw = new BufferedWriter(fw);
                        //         PrintWriter out = new PrintWriter(bw);
                        //         // System.out.println("second:   "+test._2);
                        //         out.println(test._2);
                        //         out.close();
                        //         //more code
                        //         // out.println("more text");
                        //         // //more code
                        // } catch (IOException e) {
                        //         //exception handling left as an exercise for the reader
                        // }











                        // list.add(test._1);
                }




                long count = rdd.count();
                objectsRead += count;
                System.out.println("size:    "+ objectsRead);


                String host = sparkgis.SparkGISConfig.mongoHost;
                int port = sparkgis.SparkGISConfig.mongoPort;

                MongoClient mongoClient = new MongoClient(host, port);
                DB db =  mongoClient.getDB("metadata_db");

                // System.out.println("helloworld!!!!!!!!!!!");

                // System.out.println(outputdbCollection);

                DBCollection results = db.getCollection("metadata_col");



                // for (String tmp : list )  {
                //         // System.out.println(test._1 );
                //         // System.out.println(test._2);
                //
                //         BasicDBObject wdoc = new BasicDBObject("analysis_execution_id", tmp)
                //                              .append("x", "test._1")
                //                              .append("tile_id", "dd");
                //         results.insert(wdoc);
                // }



                mongoClient.close();




                if (!started) {
                        System.out.println("saving111");
                        streamStart = System.nanoTime();
                        started = true;
                }

                System.out.println("saving222");
                // long count = rdd.count();
                // objectsRead += count;

                System.out.println("saving333");
                System.out.println("RDD Count: " + objectsRead);
                System.out.println("RDD Count: " + objectsCount);

                // update datacube either in MongoDB or in HDFS
                System.out.println("DataCube.savePath4444\n\n\n");
                // System.out.println(DataCube.savePath + "/stream");
                // rdd.saveAsTextFile(DataCube.savePath + "/stream");








                // JUST FOR PROFILLING
                if (objectsRead == objectsCount) {


                        System.out.println("equal??\n\n\n");







 String fileName = "datacube_";

  







try {


 FileOutputStream outputStream =
                        new FileOutputStream(fileName);
                                



                                // Area : 1,Perimeter : 2,Elongation : 0,/4644

                                int flag = -1;



                                for (Map.Entry<String, ArrayList<String> > entry : savedMap.entrySet())
                                {
                                        String key = entry.getKey();
                                        ArrayList<String>  value = entry.getValue();
                                        // System.out.println( key + "/" + value.size());
                                        String[] tmp_array =   key.split(",");
                                        List<String> tmp1=Arrays.asList(tmp_array);

                                        ArrayList<String> arraylist1 = new ArrayList<String>();
                                        String key_1_cat ="";
                                        String key_2_cat ="";

                                        for(String key_tmp : tmp1)
                                        {
                                                String key_1 = key_tmp.split(":")[0];
                                                String key_2 = key_tmp.split(":")[1];
                                                // arraylist1.add(key_2);
                                                key_1_cat = key_1_cat+key_1+",";
                                                key_2_cat = key_2_cat+key_2+",";
                                        }


                                        if(flag == -1) {

                                              
                                             byte[] buffer = (key_1_cat+"\n").getBytes();
                                            outputStream.write(buffer);
                                               
                                                flag = 1;

                                        }
                                          byte[] buffer = (key_2_cat+"_size:"+value.size()+"\n").getBytes();
                                            outputStream.write(buffer);
                                               

                                        for(String cell_id : value)
                                        {
                                             byte[] buffer2 = (cell_id+"\n").getBytes();
                                            outputStream.write(buffer2);
                                                
                                        }

                                }


                                outputStream.close();

                        } catch (IOException e) {
                                //exception handling left as an exercise for the reader
                        }
















                        // try {

                        //         FileWriter fw = new FileWriter("datacube", true);
                        //         BufferedWriter bw = new BufferedWriter(fw);
                        //         PrintWriter out = new PrintWriter(bw);



                        //         // Area : 1,Perimeter : 2,Elongation : 0,/4644

                        //         int flag = -1;



                        //         for (Map.Entry<String, ArrayList<String> > entry : savedMap.entrySet())
                        //         {
                        //                 String key = entry.getKey();
                        //                 ArrayList<String>  value = entry.getValue();
                        //                 // System.out.println( key + "/" + value.size());
                        //                 String[] tmp_array =   key.split(",");
                        //                 List<String> tmp1=Arrays.asList(tmp_array);

                        //                 ArrayList<String> arraylist1 = new ArrayList<String>();
                        //                 String key_1_cat ="";
                        //                 String key_2_cat ="";

                        //                 for(String key_tmp : tmp1)
                        //                 {
                        //                         String key_1 = key_tmp.split(":")[0];
                        //                         String key_2 = key_tmp.split(":")[1];
                        //                         // arraylist1.add(key_2);
                        //                         key_1_cat = key_1_cat+key_1+",";
                        //                         key_2_cat = key_2_cat+key_2+",";
                        //                 }


                        //                 if(flag == -1) {

                        //                         out.println(key_1_cat);
                        //                         flag = 1;

                        //                 }

                        //                 out.println(key_2_cat+"_size:"+value.size());
                        //                 for(String cell_id : value)
                        //                 {
                        //                         out.println(cell_id);
                        //                 }

                        //         }


                        //         out.close();

                        // } catch (IOException e) {
                        //         //exception handling left as an exercise for the reader
                        // }




























                        // stopStream();


                        // try{
                        //         Socket s=new Socket("localhost",port_num);
                        //         DataOutputStream dout=new DataOutputStream(s.getOutputStream());
                        //         dout.writeUTF("shut up streaming");
                        //         dout.flush();
                        //         // dout.close();
                        //         // s.close();
                        // }catch(Exception e) {System.out.println(e); }










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