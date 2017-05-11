package sparkgis.coordinator;
/* Java imports */
import java.io.Serializable;
/* Local imports */
import sparkgis.enums.PartitionMethod;


public class SparkGISJobConf implements Serializable{

    public static final String DUMMY_ID = "10tgaj17ga986";
    
    private String jobID;
    private int batchFactor = 1;
    private String delimiter = "\t";
    private int spatialObjectIndex = 1;
    private int partitionSize = 512;
    private PartitionMethod partitionMethod = PartitionMethod.FIXED_GRID;
    /**
     * @param jobID Sets jobID for this job
     */
    public SparkGISJobConf setJobID(String jobID){
	this.jobID = jobID;
	return this;
    }
    /**
     * @param batchFactor Sets the number of concurrent spatial jobs to execute
     * (default is 1)
     */
    public SparkGISJobConf setBatchFactor(int batchFactor){
	this.batchFactor = batchFactor;
	return this;
    }
    /**
     * @param delimiter Sets delimiter for jobs that reads spatial data from
     * text source e.g. HDFS (default TAB)
     */
    public SparkGISJobConf setDelimiter(String delimiter){
	this.delimiter = delimiter;
	return this;
    }
    /**
     * @param spatialObjectIndex Sets the index of spatial data in 'delimiter' delimited
     * text file (default is 1)
     */
    public SparkGISJobConf setSpatialObjectIndex(int spatialObjectIndex){
	this.spatialObjectIndex = spatialObjectIndex;
	return this;
    }
    /**
     * @param partitionSize Sets the distributed partition size for SparkGIS job
     * (default is 512)
     */
    public SparkGISJobConf setPartitionSize(int partitionSize){
	this.partitionSize = partitionSize;
	return this;
    }
    /**
     * @param partitioner Sets the partitioning method to be used for SparkGIS job
     * (default is FIXED_GRID)
     */
    public SparkGISJobConf setPartitionMethod(PartitionMethod partitionMethod){
	this.partitionMethod = partitionMethod;
	return this;
    }
    
    

    /**
     * @return Current jobID
     */
    public String getJobID(){return this.jobID;}
    /**
     * @return Current batch factor
     */
    public int getBatchFactor(){return this.batchFactor;}
    /**
     * @return Current delimiter for text based spatial data
     */
    public String getDelimiter(){return this.delimiter;}
    /**
     * @return The index of spatial data in 'delimiter' delimited
     * textfile
     */
    public int getSpatialObjectIndex(){return this.spatialObjectIndex;}
    /**
     * @return The partition size for distributed spatial jobs
     */
    public int getPartitionSize(){return this.partitionSize;}
    /**
     * @return The partitioner method for this jobs
     */
    public PartitionMethod getPartitionMethod(){return this.partitionMethod;}
}
