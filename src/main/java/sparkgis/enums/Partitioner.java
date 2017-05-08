package sparkgis.enums;

/**
 * Partition method to be used for spatial 
 * data distribution
 */
public enum Partitioner{
    
    FIXED_GRID,
	STEP,
	BINARY_SPACE, 
	QUAD_TREE,
	STRIP,
	HILBERT_CURVE,
	SORT_TILE_RECURSIVE;
}