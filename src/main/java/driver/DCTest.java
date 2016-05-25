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
import datacube.data.PropertyName;

public class DCTest{
    private final String db = "u24_nodejs";
    private final String collection = "objects";
    private final String analysis_exe_id = "yi-algo-v2";
    private final String caseID1 = "TCGA-CS-4941-01Z-00-DX1"; // ~65191
    private final String caseID2 = "TCGA-CS-5393-01Z-00-DX1"; // ~472518

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
    static{
	dimNames = new ArrayList<PropertyName>();
	for (int i=0; i<DataCubeMain.dimCount; ++i)
	    dimNames.add(dimensions.get(i));
    }
    
    public void start(int dimCount,
		      int bucketCount,
		      int caseIDCount
		      ){

	DataCube dc = new DataCube();
	for (int i=0; i<dimCount; ++i)
	    dc.addDimension(dimensions.get(i), bucketCount);

	LinkedHashMap<String, Object> params = objectsQuery(caseIDCount);

	//LinkedHashMap<String, String> params1 = mongoHadoopQuery();
	//System.out.println(params1);

	System.out.println(params);
	//dc.buildCube(params);

	dc.buildStreaming(params);
	
    }

    private LinkedHashMap<String, String> mongoHadoopQuery(){
	LinkedHashMap<String, String> params;
	
	params = new LinkedHashMap<String, String>();
	params.put("host", SparkGISConfig.mongoHost);
	params.put("port", String.valueOf(SparkGISConfig.mongoPort));
	params.put("db", db);
    	params.put("collection", collection);

	String queryStr = "{'provenance.analysis_execution_id':'yi-algo-v2', 'image.caseid':'TCGA-CS-4941-01Z-00-DX1'}";
	params.put("query", queryStr);

	return params;
	
    }
    
    private LinkedHashMap<String, Object> objectsQuery(int caseIDCount){
	LinkedHashMap<String, Object> params;
	
	params = new LinkedHashMap<String, Object>();

	params.put("host", SparkGISConfig.mongoHost);
	params.put("port", String.valueOf(SparkGISConfig.mongoPort));
	
    	params.put("db", db);
    	params.put("collection", collection);

	if (caseIDCount == 1){
	    params.put("provenance.analysis_execution_id", analysis_exe_id);
	    params.put("image.caseid", caseID2);
	}
	else if (caseIDCount == 2){
	    List<String> orClauses = new ArrayList<String>();
	    orClauses.add(caseID1);
	    orClauses.add(caseID2);

	    LinkedHashMap<String, List> or = new LinkedHashMap<String, List>();
	    or.put("image.caseid", orClauses);
	    params.put("or", or);
	}
	else
	    params.put("provenance.analysis_execution_id", analysis_exe_id);
	
	return params;
    }
    
}
