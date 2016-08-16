package datacube.io;
/* Java imports */
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.*;
/* Spark imports */
import org.apache.spark.api.java.JavaRDD;
/* SparkGIS core includes */
import sparkgis.SparkGIS;
import sparkgis.core.io.mongodb.Mongo;
//import sparkgis.core.io.mongodb.ReadMongoSplit;
import sparkgis.core.io.mongodb.MongoDBDataAccess;
/* MongoDB imports */
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.AggregationOutput;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBList;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
/* Local imports */
import datacube.data.DCObject;
import datacube.data.DCDimension;
import datacube.data.PropertyName;

import sparkgis.SparkGISConfig;

public class DCMongoDBDataAccess
extends MongoDBDataAccess
implements Serializable
{
private static final String host = sparkgis.SparkGISConfig.mongoHost;
private static final int port = sparkgis.SparkGISConfig.mongoPort;

public DCMongoDBDataAccess(){
}

// public DCMongoDBDataAccess(Class dataClass){
//  super(dataClass);
// }

// /**
//  * ISparkGISIO concrete function for MongoDB
//  * Simply reads in ALL the data in memory distributedly and returns an RDD
//  */
// @Override
// public JavaRDD getDataRDD(Map<String, Object> params){

//  // // get objects count from default method i.e. all qualified objects
//  // long objectCount = super.getObjectsCount(params, null);

//  // final int nSplits = SparkGIS.sc.defaultParallelism();
//  // final int splitSize = (int)objectCount/nSplits;

//  // System.out.println("Object Count: " + objectCount);
//  // System.out.println("Split Size: " + splitSize);

//  // // use the default method to get the splits for all the objects
//  // JavaRDD<Long> splits = super.getSplits(objectCount, splitSize);

//  // return splits.flatMap(new ReadMongoSplit(host, port, params, splitSize, DCObject.class));

//  super.getDataRDD(params);
// }

//public getDataRDDStream

// /**
//  * Overloaded method to allow for streaming data read from MongoDB
//  */
// public JavaRDD getDataRDD(Map<String, Object> params, Long batchStart, long batchSize){

//  // get objects count from default method i.e. all qualified objects
//  //long objectCount = super.getObjectsCount(params, null);
//  final int nSplits = SparkGIS.sc.defaultParallelism();
//  final int splitSize = (int)batchSize/nSplits;

//  JavaRDD<Long> splits = getSplits(batchStart, batchSize, splitSize);

//  return splits.flatMap(new ReadMongoSplit(host, port, params, splitSize, DCObject.class));
// }

// public JavaRDD<Long> getSplits(
//           Long batchStart,
//           Long batchSize,
//           int splitSize
//           ){

//  // create list of splits
//  List<Long> splits = new ArrayList<Long>();
//  final Long batchEnd = (batchStart + batchSize);
//  for (long i=batchStart; i<=batchEnd; i+=splitSize)
//      splits.add(i);
//  // distribute splits among nodes
//  JavaRDD<Long> splitsRDD = SparkGIS.sc.parallelize(splits);
//  return splitsRDD;
// }

/**
 * @param option 0:aggregation, 1:sort, 2:mapReduce
 */
public void getMinMax(List<DCDimension> dimensions, int option, Map<String, Object> params){

        if (
                !params.containsKey("db") ||
                !params.containsKey("collection")
                )
                throw new RuntimeException("[SparkGIS] Missing required parameters (db OR collection)");
        final String dbName = (String)params.get("db");
        final String col = (String)params.get("collection");

        System.out.println("min_max_dbName:  "+dbName);
        System.out.println("min_max_col:  "+col);

        final MongoClient mongoClient = new MongoClient(host, port);
        // check if we already have calculated results
        DBObject metaResult = metaDataExists(dbName, params, mongoClient);
        if (metaResult != null) {
                System.out.println("[Datacube] Exists .....");
                DBObject result = (DBObject)metaResult.get("min_max");
                for (DCDimension dim : dimensions) {
                        if (result.containsKey("min"+dim.getNameStr())) {
                                Double min = (Double)result.get("min"+dim.getNameStr());
                                //System.out.println("Min: " + min);
                                dim.setMin(min);
                        }
                        if (result.containsKey("max"+dim.getNameStr()))
                                dim.setMax((Double)result.get("max"+dim.getNameStr()));
                }
                //System.out.println("Result: " + metaResult);
        }
        else{

                // final List<PropertyName> features = Arrays.asList(PropertyName.values());


                HashMap hm =new HashMap();
                hm.put("Area",PropertyName.AREA);
                hm.put("Elongation",PropertyName.ELONGATION);
                hm.put("Roundness",PropertyName.ROUNDNESS);
                hm.put("Flatness",PropertyName.FLATNESS);
                hm.put("Perimeter", PropertyName.PERIMETER);
                hm.put("EquivalentSphericalRadius",PropertyName.EQUIVALENT_SPHERICAL_RADUIS);
                hm.put("EquivalentSphericalPerimeter",PropertyName.EQUIVALENT_SPHERICAL_PERIMETER);
                hm.put("EquivalentEllipsoidDiameter0",  PropertyName.EQUIVALENT_ELLIPSOID_DIAMETER0);









                List<PropertyName> features = new ArrayList<PropertyName>();


                String dimension_str =  SparkGISConfig.dimension_str;

                String[] dimensions1 = dimension_str.split(",");

                for(int i=0; i<dimensions1.length; i++)
                {
                        String tmp = dimensions1[i];
                        String key = tmp.split("_")[0];

                        PropertyName tmpPN = (PropertyName)(hm.get(key));


                        features.add(tmpPN );
                }












                List<String> sortResults = new ArrayList<String>();

                try{

                        DB db =  mongoClient.getDB(dbName);
                        DBCollection collection = db.getCollection(col);


// http://mongodb.github.io/mongo-java-driver/2.13/getting-started/quick-tour/
                        // DBObject matchValues = Mongo.prepareQuery(params);  "Area" : 851,
                        // DBObject matchValues = Mongo.prepareQuery(params).append(  "features.Perimeter",121.55999755859375   ).append(  "features.Area",851   );

                        // DBObject matchValues = Mongo.prepareQuery(params).append(  "features.Perimeter", new BasicDBObject("$gt", 120).append("$lte", 122)   ).append(  "features.Area",851   );
                        DBObject matchValues = Mongo.prepareQuery(params);


                        // features" : { "Perimeter" : 121.55999755859375

                        //new BasicDBObject("provenance.analysis_execution_id", "yi-algo-v2");
                        //matchValues.put("image.caseid", "TCGA-CS-4941-01Z-00-DX1");
                        DBObject match = new BasicDBObject("$match", matchValues);

//                         query = new BasicDBObject("i", new BasicDBObject("$gt", 20).append("$lte", 30));
// cursor = coll.find(query);

                        if (option == 0) {
                                DBObject groupValues = new BasicDBObject( "_id", "{}");

                                for (PropertyName pName : features) {

                                        if (pName.value.equals("image.caseid"))
                                                continue;
                                        groupValues.put(("min" + pName.value), new BasicDBObject("$min", ("$features."+pName.value)));
                                        groupValues.put(("max" + pName.value), new BasicDBObject("$max", ("$features."+pName.value)));
                                }
                                DBObject group = new BasicDBObject("$group", groupValues);

                                long start = System.nanoTime();

                                AggregationOutput minMax = collection.aggregate(match, group);

                                // save to speedup future uses
                                insertMetaData(dbName, params, minMax, mongoClient);

                                System.out.println(TimeUnit.SECONDS.convert(System.nanoTime()-start, TimeUnit.NANOSECONDS) + " s");

                                for (final DBObject result : minMax.results()) {
                                        for (DCDimension dim : dimensions) {
                                                if (result.containsKey("min"+dim.getNameStr()))
                                                        dim.setMin((Double)result.get("min"+dim.getNameStr()));
                                                if (result.containsKey("max"+dim.getNameStr()))
                                                        dim.setMax((Double)result.get("max"+dim.getNameStr()));
                                        }
                                        //System.out.println("Result: " + result);
                                }
                        }
                        // just for getting numbers
                        // else if (option == 1){
                        //  for (PropertyName pName:features){
                        //      DBCursor cur = collection.find(matchValues).sort(new BasicDBObject("features."+pName.value, 1)).limit(1);
                        //      if (cur.hasNext())
                        //    sortResults.add(cur.next().toString());
                        //  }d
                        //  for (final String res : sortResults)
                        //      System.out.println(res);
                        // }

                }catch(Exception e) {System.out.println(e.toString()); }
        }
}

private DBObject metaDataExists(
        final String dbName,
        final Map<String, Object> dataParams,
        //final AggregationOutput minMax,
        final MongoClient mongoClient){

        try{
                //final MongoClient mongoClient = new MongoClient(host , port);

                DB db =  mongoClient.getDB(dbName);
                DBCollection collection = db.getCollection("datacube");

                BasicDBObject dataCubeMetaData = new BasicDBObject();

                BasicDBObject metaData = new BasicDBObject();
                Iterator iterator = dataParams.entrySet().iterator();
                while(iterator.hasNext()) {
                        Map.Entry entry = (Map.Entry)iterator.next();
                        //if (entry.getKey().equals("db") || entry.getKey().equals("collection"))
                        //    continue;
                        String key = ((String)entry.getKey()).replaceAll("\\.", "__");
                        Object value = entry.getValue();
                        if (value instanceof String) {
                                metaData.append(key, (String)value);
                        }
                        else if(value instanceof Map) {
                                @SuppressWarnings("unchecked")
                                BasicDBList clauses = Mongo.getClauses((Map<String, Object>)value, true);
                                metaData.append(key, clauses);
                        }
                }
                dataCubeMetaData.append("meta_data", metaData);

                return collection.findOne(dataCubeMetaData);
                //return true;

        }catch(Exception e) {e.printStackTrace(); }
        return null;
}
private void insertMetaData(
        final String dbName,
        final Map<String, Object> dataParams,
        final AggregationOutput minMax,
        final MongoClient mongoClient
        ){

        try{
                //final MongoClient mongoClient = new MongoClient(host , port);

                DB db =  mongoClient.getDB(dbName);
                DBCollection collection = db.getCollection("datacube");
                DBCollection collection1 = db.getCollection("meta");

                BasicDBObject dataCubeMetaData = new BasicDBObject();
                BasicDBObject dataCubeMetaData2 = new BasicDBObject();


                BasicDBObject metaData = new BasicDBObject();
                Iterator iterator = dataParams.entrySet().iterator();
                while(iterator.hasNext()) {
                        Map.Entry entry = (Map.Entry)iterator.next();
                        //if (entry.getKey().equals("db") || entry.getKey().equals("collection"))
                        //    continue;
                        String key = ((String)entry.getKey()).replaceAll("\\.", "__");
                        Object value = entry.getValue();
                        if (value instanceof String) {
                                metaData.append(key,(String)value);
                        }
                        else if(value instanceof Map) {
                                @SuppressWarnings("unchecked")
                                BasicDBList clauses = Mongo.getClauses((Map<String, Object>)value, true);
                                metaData.append(key, clauses);
                        }
                }

                dataCubeMetaData.append("meta_data", metaData);
                for (final DBObject result : minMax.results()) {
                        dataCubeMetaData.append("min_max", result);
                }

                collection.insert(dataCubeMetaData);


                // String dimension_str =  SparkGISConfig.dimension_str;
                //
                // String[] dimensions = dimension_str.split(",");
                //
                // for(int i=0; i<dimensions.length; i++)
                // {
                //         String tmp = dimensions[i];
                //         String key = tmp.split("_")[0];
                //
                //         PropertyName tmpPN = (PropertyName)(hm.get(key));
                //
                //         String value =   tmp.split("_")[1];
                //         Integer value_int = Integer.valueOf(value);
                //
                //         dc.addDimension(tmpPN,value_int);
                // }
                //
                //
                //
                //
                // String[] query_range = SparkGISConfig.query_range.split("-");
                //
                // for(int i=0; i<query_range.length; i++)
                // {
                //         String tmp = query_range[i];
                //         String key = tmp.split("_")[0];
                //
                //
                //         String value =   tmp.split("_")[1];
                //
                //
                //         params.put("features."+key, value);
                // }














        }catch(Exception e) {e.printStackTrace(); }
}
}
