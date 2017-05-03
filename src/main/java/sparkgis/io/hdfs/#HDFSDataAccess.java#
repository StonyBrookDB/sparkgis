package sparkgis.io.hdfs;
/* Java imports */
import java.io.*;
import java.io.Serializable;
import java.net.URI;
//import java.lang.Runnable;
import java.util.List;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
/* Spark imports */
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
/* HDFS imports */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
//import org.apache.hadoop.fs.OutputStream;
import org.apache.hadoop.util.Progressable;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.data.Tile;
import sparkgis.io.ISparkGISIO;
import sparkgis.data.TileStats;
import sparkgis.data.DataConfig;
//import sparkgis.storagelayer.DataAccessLayer;
//import sparkgis.data.RawData;
import sparkgis.data.Polygon;
import sparkgis.data.HDFSPolygon;

public class HDFSDataAccess implements ISparkGISIO, Serializable
{
    private static final char TAB = '\t';    
    // default HDFS config for BMI Cluster
    //private final String hdfsPrefix = "hdfs://10.10.10.11/user/"; // nfs001 - infiniband
    private final String hdfsPrefix = "hdfs://130.245.124.11/user/"; // nfs001 - gigabit
    private String coreSitePath = "/home/hoang/nfsconfig/core-site.xml";
    private String hdfsSitePath = "/home/hoang/nfsconfig/hdfs-site.xml";
    private String dataDir = hdfsPrefix + "fbaig/new-data/";
    private String resultsDir = hdfsPrefix + "fbaig/results/";

    public HDFSDataAccess(){}
    /**
     * @param coreSitePath Hadoop configuration path for core-site.xml
     * @param hdfsSitePath Hadoop configuration path for hdfs-site.xml
     * @param dataDir Prefix hdfs path for data directory (Might note be needed)
     * @param outDir Prefix hdfs path to write results to
     */
    public HDFSDataAccess(String coreSitePath, String hdfsSitePath, String dataDir, String outDir){
	this.coreSitePath = coreSitePath;
	this.hdfsSitePath = hdfsSitePath;
	this.dataDir = dataDir;
	this.resultsDir = outDir;
    }
    
    /**
     * Reads input data and return as it is
     * @param datapath HDFS path for data to read
     * @param persist Boolean value to keep RDD in memory or not
     * @return JavaRDD String RDD of data
     */
    private JavaRDD<String> getDataAsText(String datapath){
	JavaRDD<String> rawTextData = 
	    SparkGIS.sc.textFile(dataDir + "/" + datapath, SparkGIS.sc.defaultParallelism()).filter(new Function<String, Boolean>(){
		    public Boolean call(String s) {
			return (!s.isEmpty() && (s.split("\t").length >= 2));
		    }
		});
	return rawTextData;
    }
    // /**
    //  * Reads input data and replace all instaces of orignal delimiter with TABS '\t'
    //  * Map: Replace orginal delimiter with TAB
    //  * Reduce: None
    //  * @param datapath HDFS path for data to read
    //  * @param origDelimiter Delimiter character in original data
    //  * @param persist Boolean value to keep RDD in memory or not
    //  * @return JavaRDD Tab Seperated String RDD of data
    //  */
    // public JavaRDD<String> getDataAsTSV(String datapath, char origDelimiter){
    // 	final char oDelim = origDelimiter;
    // 	JavaRDD<String> tsvData = 
    // 	    getDataAsText(datapath).map(new Function <String, String>() {
    // 	    	public String call(String s) { return s.replace(oDelim, TAB);} 
    // 	    });
    // 	return tsvData;
    // }

    // public JavaRDD<Polygon> getData(String algo, String image){
    // 	return getDataAsText(algo+"/"+image).map(new Function<String, Polygon>(){
    // 		public Polygon call(String s){
    // 		    String[] fields = s.split("\t");
    // 		    return new HDFSPolygon(fields[0], fields[1]);
    // 		}
    // 	    });
    // }
    
    public JavaRDD<Polygon> getPolygonsRDD(String caseID, String algo){
	return getDataAsText(algo+"/"+caseID).map(new Function<String, Polygon>(){
		public Polygon call(String s){
		    String[] fields = s.split("\t");
		    return new HDFSPolygon(fields[0], fields[1]);
		}
	    });
    }

    public String writeTileStats(JavaRDD<TileStats> data, String... args){
	String caseID = args[0];
	data.saveAsTextFile(resultsDir + "/" + caseID);
	return (resultsDir + "/" + caseID);
    }
    
    // public void writeTileStats(List<TileStats> data, String caseID){
    // 	try{
    // 	    Configuration config = new Configuration();
    // 	    config.addResource(new Path(coreSitePath));
    // 	    config.addResource(new Path(hdfsSitePath));
    // 	    FileSystem fs = FileSystem.get(config);
    // 	    URI pathURI = URI.create(resultsDir + "/" + caseID);
    // 	    Path path = new Path(pathURI);
    // 	    FSDataOutputStream fos = fs.create(path);
    // 	    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( fos, "UTF-8" ) );
    // 	    for (TileStats ts : data){
    // 		br.write(ts.avgJaccardCoEff + "\t" + ts.tile.toString());
    // 	    }
    // 	    br.close();
    // 	    fs.close();
    // 	} catch(Exception e) {e.printStackTrace();}
    // }
    
    /**
     * Called from MongoToHDFS
     */
    public void writePolygons(List<Polygon> pList, String filePath){
	//String dataDir = hdfsPrefix + "fbaig/new-data/";
	try{
    	    Configuration config = new Configuration();
    	    config.addResource(new Path(coreSitePath));
    	    config.addResource(new Path(hdfsSitePath));
    	    FileSystem fs = FileSystem.get(config);
    	    URI pathURI = URI.create(dataDir + "/" + filePath);
    	    Path path = new Path(pathURI);
    	    FSDataOutputStream fos = fs.create(path);
    	    BufferedWriter pWriter = new BufferedWriter( new OutputStreamWriter( fos, "UTF-8" ) );
	    for (Polygon p : pList)
		pWriter.write(p.toString() + "\n");
	    pWriter.close();
	    fs.close();
	}catch(Exception e){e.printStackTrace();}
    }
    
    // public List<String> getSplits(String splitFilePath){
    // 	List<String> ret = new ArrayList<String>();

    // 	try{
    // 	    Configuration config = new Configuration();
    // 	    config.addResource(new Path(coreSitePath));
    // 	    config.addResource(new Path(hdfsSitePath));
    // 	    Path pt = new Path(splitFilePath);
    // 	    FileSystem fs = FileSystem.get(config);
    // 	    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    // 	    String line;
    // 	    line=br.readLine();
    // 	    while (line != null){
    // 		//System.out.println(line);
    // 		//line=br.readLine();
    // 		ret.add(br.readLine());
    // 	    }
	    
    // 	}catch (Exception e){e.printStackTrace();}
    // 	return ret;
    // }

    // public List<String> getFiles(String dirPath){
    // 	List<String> fileList = new ArrayList<String>();

    // 	try{
    // 	    Configuration config = new Configuration();
    // 	    config.addResource(new Path(coreSitePath));
    // 	    config.addResource(new Path(hdfsSitePath));
    // 	    FileSystem fs = FileSystem.get(config);
    // 	    // 2nd parameter: goto subdirectories
    // 	    RemoteIterator<LocatedFileStatus> fsListIter = fs.listFiles(new Path(dirPath), false);
    // 	    while(fsListIter.hasNext()){
    // 		LocatedFileStatus fileStatus = fsListIter.next();
    // 		//do stuff with the file like ...
    // 		fileList.add(fileStatus.getPath().toString());
    // 	    }
    // 	    fs.close();
    // 	    // FileStatus[] fileStatus = fs.listStatus(dirPath);
    // 	    // for (FileStatis fileStat : fileStatus){
    // 	    // 	fileList.add(fileStat.getPath().toString());
    // 	    // }
    // 	} catch(Exception e) {e.printStackTrace();}
    // 	return fileList;
    // }

    public boolean fileExists(String filePath){
    	boolean exists = false;
    	try{
    	    Configuration config = new Configuration();
    	    config.addResource(new Path(coreSitePath));
    	    config.addResource(new Path(hdfsSitePath));
    	    FileSystem fs = FileSystem.get(config);
    	    URI pathURI = URI.create(dataDir + "/" + filePath);
    	    Path path = new Path(pathURI);
    	    exists = fs.exists(path);
    	    fs.close();
    	} catch(Exception e) {e.printStackTrace();}
    	return exists;
    }
    
    // public void writeRawData(RawData data, String filePath){
    // 	long start = System.nanoTime();
    // 	try{
    // 	    Configuration config = new Configuration();
    // 	    config.addResource(new Path(coreSitePath));
    // 	    config.addResource(new Path(hdfsSitePath));
    // 	    FileSystem fs = FileSystem.get(config);
    // 	    URI pathURI = URI.create(dataDir + "/" + filePath);
    // 	    Path path = new Path(pathURI);
    // 	    FSDataOutputStream fos = fs.create(path);
    // 	    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( fos, "UTF-8" ) );
    // 	    for (Polygon p : data.getPolygons())
    // 		br.write(p.toString() + "\n");
    // 	    br.close();
    // 	    fs.close();
    // 	} catch(Exception e) {e.printStackTrace();}
    // 	// set time taken to write data to HDFS
    // 	data.setHdfsDuration(System.nanoTime()-start);
    // }
    
    // public BufferedWriter getPolygonWriter(String filePath){
    // 	try{
    // 	    Configuration config = new Configuration();
    // 	    config.addResource(new Path(coreSitePath));
    // 	    config.addResource(new Path(hdfsSitePath));
    // 	    FileSystem fs = FileSystem.get(config);
    // 	    URI pathURI = URI.create(dataDir + "/" + filePath);
    // 	    Path path = new Path(pathURI);
    // 	    FSDataOutputStream fos = fs.create(path);
    // 	    return (new BufferedWriter( new OutputStreamWriter( fos, "UTF-8" ) ));
    // 	}catch(Exception e){e.printStackTrace();}
    // 	return null;
    // }
    // public void closePolygonWriter(BufferedWriter pWriter){
    // 	try{
    // 	    pWriter.close();
    // 	}catch(Exception e){e.printStackTrace();}
    // }
    // public void writePolygon(Polygon p, BufferedWriter pWriter){
    // 	try{
    // 	    pWriter.write(p.toString() + "\n");
    // 	}catch(Exception e){e.printStackTrace();}
    // }    

    /* bkps */
    
    // public JavaPairRDD<String, String> getDirectoryAsTSV(String dirPath, char originalDelim){
    // 	JavaPairRDD<String, String> files = SparkGIS.sc.wholeTextFiles(dirPath);
    // 	if (originalDelim == TAB)
    // 	    return files;
    // 	else{
    // 	    final char oDelim = originalDelim;
    // 	    JavaPairRDD<String, String> tsvFiles = 
    // 		files.mapValues(new Function<String, String>(){
    // 			public String call (String s) {return s.replace(oDelim, TAB);}
    // 		    });
    // 	    return tsvFiles;
    // 	}
    // }

        // /**
    //  * Write RDD as text to storage
    //  * @param data RDD to store as text file
    //  * @param path HDFS path to store data (filename inclusive)
    //  */
    // public < E > void saveRDDAsText(JavaRDD<E> data, String path){
    // 	data.saveAsTextFile(path);
    // }

    // public void saveDataConfig(DataConfig dataConfig, String partfilePath){
    // 	try{
    // 	    Configuration config = new Configuration();
    // 	    config.addResource(new Path(coreSitePath));
    // 	    config.addResource(new Path(hdfsSitePath));
    // 	    FileSystem fs = FileSystem.get(config);
	    
    // 	    //URI uri = URI.create(destPrefix + "/partfile.idx");
    // 	    URI uri = URI.create(partfilePath);
    // 	    Path partfile = new Path(uri);
    // 	    try{
    // 		FSDataOutputStream partfileOut = fs.create(partfile);
    // 		for (Tile t : dataConfig.getPartitionIDX()){		    
    // 		    //SparkGIS.Debug(str);
    // 		    partfileOut.writeChars(t.toString());
    // 		}
    // 		partfileOut.close();
    // 	    }
    // 	    catch (Exception e) {e.printStackTrace();}
	    
    // 	    // write mapped data to disk
    // 	    //	    dataConfig.mappedPartitions.partitionBy(new HashPartitioner(dataConfig.getPartitionsCount())).saveAsTextFile(destPrefix + "/data");
	    
    // 	    SparkGIS.Debug("Finished writing to disk ...");
    // 	}catch(Exception e) {e.printStackTrace();}
    // }
    
    // /**
    //  * Return input file size on HDFS in bytes
    //  * @return long Input file size in bytes
    //  */
    // public long getInputFileSize(String filePath){
	
    // 	long fileSize = 0;
    // 	try{
    // 	    Configuration config = new Configuration();
    // 	    config.addResource(new Path(coreSitePath));
    // 	    config.addResource(new Path(hdfsSitePath));
    // 	    FileSystem fs = FileSystem.get(config);
    // 	    URI pathURI = URI.create(dataDir + "/" + filePath);
    // 	    Path path = new Path(pathURI);
    // 	    fileSize = fs.getFileStatus(path).getLen();
    // 	    fs.close();
    // 	} catch(Exception e) {e.printStackTrace();}
    // 	return fileSize;
    // }  
}
