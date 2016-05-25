package sparkgis;
/* Local imports*/
import sparkgis.core.data.Tile;
import sparkgis.core.data.Polygon;
import sparkgis.core.data.TileStats;
import sparkgis.core.data.SPJResult;
import sparkgis.core.data.DataConfig;
//import sparkgis.core.io.mongodb.ReadMongoSplit;
//import sparkgis.io.hdfs.HDFSDataAccess;
//import sparkgis.io.mongodb.MongoDBDataAccess;
import sparkgis.core.executionlayer.Partitioner;
import sparkgis.pia.Coefficient;
import sparkgis.core.executionlayer.SparkPrepareData;
import sparkgis.core.executionlayer.SparkSpatialJoin;
import sparkgis.pia.SparkSpatialJoinHM;
import sparkgis.core.executionlayer.spatialindex.IndexedGeometry;
import sparkgis.core.executionlayer.spatialindex.SparkSpatialIndex;
/* Kryo Serializer */
import org.apache.spark.serializer.KryoRegistrator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
/* 
 * Kryo Serializer
 */
public class KryoClassRegistrator implements KryoRegistrator{
    public void registerClasses (Kryo kryo){
	// sparkgis.data.*
	kryo.register(Tile.class, new FieldSerializer(kryo, Tile.class));
	kryo.register(Polygon.class, new FieldSerializer(kryo, Polygon.class));
	kryo.register(TileStats.class, new FieldSerializer(kryo, TileStats.class));
	kryo.register(SPJResult.class, new FieldSerializer(kryo, SPJResult.class));
	kryo.register(DataConfig.class, new FieldSerializer(kryo, DataConfig.class));
	// sparkgis.executionlayer.*
	kryo.register(Partitioner.class, new FieldSerializer(kryo, Partitioner.class));
	kryo.register(Coefficient.class, new FieldSerializer(kryo, Coefficient.class));
	kryo.register(SparkPrepareData.class, new FieldSerializer(kryo, SparkPrepareData.class));
	kryo.register(SparkSpatialJoin.class, new FieldSerializer(kryo, SparkSpatialJoin.class));
	kryo.register(SparkSpatialJoinHM.class, new FieldSerializer(kryo, SparkSpatialJoinHM.class));
	// sparkgis.io.*
	//kryo.register(ReadMongoSplit.class, new FieldSerializer(kryo, ReadMongoSplit.class));
	//kryo.register(HDFSDataAccess.class, new FieldSerializer(kryo, HDFSDataAccess.class));
	//kryo.register(MongoDBDataAccess.class, new FieldSerializer(kryo, MongoDBDataAccess.class));
	// sparkgis.executionlayer.sparkspatialindex.*
	kryo.register(IndexedGeometry.class, new FieldSerializer(kryo, IndexedGeometry.class));
	kryo.register(SparkSpatialIndex.class, new FieldSerializer(kryo, SparkSpatialIndex.class));	   

    }
}
