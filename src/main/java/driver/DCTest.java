package driver;
/* Java imports */
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
/* SparkGIS imports */
import sparkgis.SparkGISConfig;
/* Local imports */
import datacube.DataCube;
import datacube.DataCubeStreaming;
import datacube.data.PropertyName;
import java.util.*;

public class DCTest {
// private final String db = "u24_nodejs";
// private final String collection = "objects";




private final String db = null;
private final String collection = null;

private final String analysis_exe_id = "yi-algo-v2";
private final String caseID1 = "TCGA-CS-4941-01Z-00-DX1";     // ~65191
private final String caseID2 = "TCGA-CS-5393-01Z-00-DX1";     // ~472518

private static final List<PropertyName> dimensions =
        Arrays.asList(
                PropertyName.AREA,
                PropertyName.ELONGATION,
                PropertyName.ROUNDNESS,
                PropertyName.FLATNESS,
                PropertyName.PERIMETER,
                PropertyName.EQUIVALENT_SPHERICAL_RADUIS,
                PropertyName.EQUIVALENT_SPHERICAL_PERIMETER,
                PropertyName.EQUIVALENT_ELLIPSOID_DIAMETER0
                );

// for experimentation
public static List<PropertyName> dimNames;
static {
        dimNames = new ArrayList<PropertyName>();
        for (int i=0; i<DataCubeMain.dimCount; ++i)
                dimNames.add(dimensions.get(i));
}

// public void start(int dimCount,
//                   int bucketCount,
//                   int caseIDCount
//                   ){
//
//         DataCube dc = new DataCube();
//         for (int i=0; i<dimCount; ++i)
//                 dc.addDimension(dimensions.get(i), bucketCount);
//
//         LinkedHashMap<String, Object> params = objectsQuery(caseIDCount);
//
//         //LinkedHashMap<String, String> params1 = mongoHadoopQuery();
//         //System.out.println(params1);
//
//         System.out.println(params);
//         dc.buildCube(params);
//
// }

public void startStreaming(int dimCount,
                           int bucketCount,
                           List<String> caseIDs ){

        DataCubeStreaming dc = new DataCubeStreaming();
        for (int i=0; i<dimCount; ++i)
                dc.addDimension(dimensions.get(i), bucketCount);


        // dc.addDimension(  PropertyName.AREA, 200);
        // dc.addDimension(  PropertyName.PERIMETER, 600);
        // dc.addDimension(  PropertyName.ELONGATION, 300);



        LinkedHashMap<String, Object> params = objectsQuery(caseIDs);

        System.out.println(params);

        dc.buildStreaming(params);
}


public void startStreaming_2(
        List<String> caseIDs ){

        // AREA ("Area"),
        // ELONGATION ("Elongation"),
        // ROUNDNESS ("Roundness"),
        // PHYSICAL_SIZE ("PhysicalSize"),
        // FLATNESS ("Flatness"),
        // EQUIVALENT_SPHERICAL_RADUIS ("EquivalentSphericalRadius"),
        // EQUIVALENT_SPHERICAL_PERIMETER ("EquivalentSphericalPerimeter"),
        // EQUIVALENT_ELLIPSOID_DIAMETER0 ("EquivalentEllipsoidDiameter0"),
        // EQUIVALENT_ELLIPSOID_DIAMETER1 ("EquivalentEllipsoidDiameter1"),
        // PERIMETER ("Perimeter"),
        // NUM_OF_PIXELS ("NumberOfPixels"),
        // NUM_OF_PIXELS_ON_BORDER ("NumberOfPixelsOnBorder"),
        // PRINCIPAL_MOMENTS0 ("PrincipalMoments0"),
        // PRINCIPAL_MOMENTS1 ("PrincipalMoments1"),
        // FERET_DIAMETER ("FeretDiameter")



        HashMap hm =new HashMap();
        hm.put("Area",PropertyName.AREA);
        hm.put("Elongation",PropertyName.ELONGATION);
        hm.put("Roundness",PropertyName.ROUNDNESS);
        hm.put("Flatness",PropertyName.FLATNESS);
        hm.put("Perimeter", PropertyName.PERIMETER);
        hm.put("EquivalentSphericalRadius",PropertyName.EQUIVALENT_SPHERICAL_RADUIS);
        hm.put("EquivalentSphericalPerimeter",PropertyName.EQUIVALENT_SPHERICAL_PERIMETER);
        hm.put("EquivalentEllipsoidDiameter0",  PropertyName.EQUIVALENT_ELLIPSOID_DIAMETER0);


        DataCubeStreaming dc = new DataCubeStreaming();
        // for (int i=0; i<dimCount; ++i)
        //         dc.addDimension(dimensions.get(i), bucketCount);


        String dimension_str =  SparkGISConfig.dimension_str;

        // dc.addDimension(  PropertyName.AREA, 200);
        // dc.addDimension(  PropertyName.PERIMETER, 600);
        // dc.addDimension(  PropertyName.ELONGATION, 300);










        String[] dimensions = dimension_str.split(",");

        for(int i=0; i<dimensions.length; i++)
        {
                String tmp = dimensions[i];
                String key = tmp.split("_")[0];

                PropertyName tmpPN = (PropertyName)(hm.get(key));

                String value =   tmp.split("_")[1];
                Integer value_int = Integer.valueOf(value);

                dc.addDimension(tmpPN,value_int);
        }




        LinkedHashMap<String, Object> params = objectsQuery(caseIDs);

        System.out.println(params);

        dc.buildStreaming(params);
}




private LinkedHashMap<String, String> mongoHadoopQuery(){
        LinkedHashMap<String, String> params;

        params = new LinkedHashMap<String, String>();
        params.put("host", SparkGISConfig.mongoHost);
        params.put("port", String.valueOf(SparkGISConfig.mongoPort));
        params.put("db", SparkGISConfig.db);
        params.put("collection", SparkGISConfig.collection);

        String queryStr = "{'provenance.analysis_execution_id':'yi-algo-v2', 'image.caseid':'TCGA-CS-4941-01Z-00-DX1'}";
        params.put("query", queryStr);

        return params;

}

private LinkedHashMap<String, Object> objectsQuery(List<String> caseIDs){
        LinkedHashMap<String, Object> params;

        params = new LinkedHashMap<String, Object>();

        params.put("host", SparkGISConfig.mongoHost);
        params.put("port", String.valueOf(SparkGISConfig.mongoPort));

        params.put("db", SparkGISConfig.db);
        params.put("collection", SparkGISConfig.collection);

        // params.put("features.Perimeter", "120,122");
        // params.put("features.Area", "850.5,851.5");




        String[] query_range = SparkGISConfig.query_range.split("-");

        for(int i=0; i<query_range.length; i++)
        {
                String tmp = query_range[i];
                String key = tmp.split("_")[0];


                String value =   tmp.split("_")[1];


                params.put("features."+key, value);
        }



        // "features.Perimeter", new BasicDBObject().append("$gt", 120).append("$lte", 122)   ).append(  "features.Area",851   );
        // cursor = coll.find(query);

        // params.put("provenance.analysis_execution_id", analysis_exe_id);
        // params.put("image.caseid", caseID1);


        LinkedHashMap<String, List> or = new LinkedHashMap<String, List>();
        or.put("image.caseid", caseIDs);
        params.put("or", or);





        // if (caseIDCount == 1) {
        //         params.put("provenance.analysis_execution_id", analysis_exe_id);
        //         params.put("image.caseid", caseID1);
        // }
        // else if (caseIDCount == 2) {
        //         List<String> orClauses = new ArrayList<String>();
        //         orClauses.add(caseID1);
        //         orClauses.add(caseID2);
        //
        //         LinkedHashMap<String, List> or = new LinkedHashMap<String, List>();
        //         or.put("image.caseid", orClauses);
        //         params.put("or", or);
        // }
        // else
        //         params.put("provenance.analysis_execution_id", analysis_exe_id);

        return params;
}

}
