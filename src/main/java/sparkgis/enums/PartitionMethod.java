package sparkgis.enums;

/**
 * Partition method to be used for spatial 
 * data distribution
 */
public enum PartitionMethod{
    
    FIXED_GRID,
    FIXED_GRID_HM,
    BINARY_SPACE, 
    QUAD_TREE,
    STRIP,
    HILBERT_CURVE,
    SORT_TILE_RECURSIVE;
}
