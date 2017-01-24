package sparkgis.executionlayer.partitioning;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.lang.Math;
/* Local imports */
import sparkgis.SparkGIS;
import sparkgis.data.Tile;

public class Partitioner
{
    /**
     * Method: fixed-grid partitioning
     * @param spaceXSpan (maxX - minX)
     * @param spaceYSpan (maxY - minY)
     * @param partitionSize Bucket size for this partitioning
     * @parm numObjects Total objects count
     * @return List<String> List of normalized MBR of regions of partitions 
     */
    public static List<Tile> fixedGrid(double spaceXSpan, double spaceYSpan, int partitionSize, long numObjects){
	
	double xSplit = 1, ySplit = 1;
	double xTile, yTile;
	long id = 0;
	List<Tile> partitions = new ArrayList<Tile>();
	
	/* Determine number of splits along x and y axis */
	if (spaceYSpan > spaceXSpan){
	    ySplit = Math.max(Math.ceil(Math.sqrt((double)numObjects / partitionSize * (spaceYSpan/spaceXSpan))), 1.0);
	    xSplit = Math.max(Math.ceil(((double)numObjects / partitionSize) / ySplit), 1.0);
	}
	else{
	    xSplit = Math.max(Math.ceil(Math.sqrt((double)numObjects / partitionSize * (spaceXSpan/spaceYSpan))), 1.0);
	    ySplit = Math.max(Math.ceil((double)numObjects / partitionSize / xSplit), 1.0);
	}
	
	xTile = (1.0 / xSplit);
	yTile = (1.0 / ySplit);
	
	for (int x=0; x<xSplit; ++x){
	    for (int y=0; y<ySplit; ++y){
		id++;
		Tile t = new Tile();
		t.tileID = id;
		t.minX = (x*xTile);
		t.minY = (y*yTile);
		t.maxX = ((x+1)*xTile);
		t.maxY = ((y+1)*yTile);
		partitions.add(t);
		//partitions.add(SparkGIS.createTSString(id, (x*xTile), (y*yTile), ((x+1)*xTile), ((y+1)*yTile)));
	    }
	}
	return partitions;
    }

    /**
     * Fixed-grid partition for heatmap generation
     */
    public static List<Tile> fixedGridHM(double minX, double minY, double maxX, double maxY, int step){
	long id = 0;
	//List<String> partitions = new ArrayList<String>();
	List<Tile> partitions = new ArrayList<Tile>();
	
	for (int i=(int)minX; i<=maxX; i+=step){
	    for (int j=(int)minY; j<=maxY; j+=step){
		id++;
		Tile t = new Tile();
		t.tileID = id;
		t.minX = i;
		t.minY = j;
		t.maxX = (i+step);
		t.maxY = (j+step);
		partitions.add(t);
		//partitions.add(SparkGIS.createTSString(id, i, j, (i+step), (j+step)));
	    }
	}
	return partitions;
    }
}
