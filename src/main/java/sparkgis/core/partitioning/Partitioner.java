package sparkgis.core.partitioning;
/* Java imports */
import java.util.List;
import java.util.ArrayList;
import java.lang.Math;
/* Local imports */
import sparkgis.data.Tile;

public class Partitioner
{
    /**
     * Method: fixed-grid partitioning
     * @param spaceXSpan (maxX - minX)
     * @param spaceYSpan (maxY - minY)
     * @param partitionSize Bucket size for this partitioning
     * @param numObjects Total objects count
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
	    }
	}
	return partitions;
    }

    /**
     * Fixed-grid partition for heatmap generation
     * NOTE: DONT DENORMALIZE IF USING THIS FUNCTION
     */
    public static List<Tile> fixedGridHM(double minX, double minY, double maxX, double maxY, int partitionSize){
	long id = 0;
	List<Tile> partitions = new ArrayList<Tile>();
	final double xStep = (maxX-minX)/(partitionSize-1);
	final double yStep = (maxY-minY)/(partitionSize-1);

	maxX = Math.ceil(maxX);
	maxY = Math.ceil(maxY);

	for (int i=(int)minX; i<=maxX; i+=xStep){
	    for (int j=(int)minY; j<=maxY; j+=yStep){
		id++;
		Tile t = new Tile();
		t.tileID = id;
		t.minX = i;
		t.minY = j;
		t.maxX = (i+xStep);
		t.maxY = (j+yStep);
		partitions.add(t);
	    }
	}
	return partitions;
    }
}
