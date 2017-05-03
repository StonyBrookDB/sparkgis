package sparkgis.executionlayer.spatialindex;
/* Java imports */
import java.io.Serializable;
/* JTS imports */
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;

public class IndexedGeometry implements Serializable{
    private final long key;
    //private final PreparedGeometry geom;
    private final Geometry geom;

    public IndexedGeometry(long key, Geometry geom){
	this.key = key;
	this.geom = geom;//PreparedGeometryFactory.prepare(geom);
    }
    public long getKey() {return key;}
}
