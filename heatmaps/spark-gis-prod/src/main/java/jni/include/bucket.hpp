#ifndef BUCKET_H
#define BUCKET_H

/* geos */
#include <geos/geom/Geometry.h>
/* libspatialindex */
#include <spatialindex/SpatialIndex.h>
/* local */
#include "rtree.hpp"

using namespace SpatialIndex;
using namespace geos::geom;

/*
 * Data structures required for a single bucket processing
 * For 2D the bucket corresponds to a tile having spatial 
 * objects from all datasets
 */
class Bucket{

public:
  
  ISpatialIndex * spidx;
  IStorageManager * storage;

  bool selfjoin;
  int idx1; 
  int idx2;
  
  /* spatial query result in string form */
  vector<string> ret_vec;

  std::vector<Geometry*>  poly_set_one;                                   
  std::vector<Geometry*>  poly_set_two;

  size_t len1;
  size_t len2;

  /*
   * Overloaded constructor for bucket with all data
   */
  Bucket(struct query_op &stop, map<int, vector<Geometry*> > &polydata)
  {
    spidx = NULL;
    storage = NULL;
    
    selfjoin = (stop.join_cardinality == 1) ? true : false ;
      
    idx1 = SID_1;
    idx2 = selfjoin ? SID_1 : SID_2;

    poly_set_one = polydata[idx1];
    poly_set_two = polydata[idx2];

    len1 = polydata[idx1].size();
    len2 = polydata[idx2].size();
    
  }

  /*
   * Return all spatial objects belonging to given dataset
   */
  std::vector<Geometry*>  & get_dataset(int set){
    if (set == 1) return poly_set_one;
    else if (set == 2) return poly_set_two;
    throw std::runtime_error("[bucket.hpp] Request for invalid dataset");
  }

  /*
   * Build R-Tree index on given dataset
   */
  bool build_rtree_index(int dataset){
    std::vector<Geometry*>  & poly_set = get_dataset(dataset);
    const size_t size = poly_set.size();
    /* make a copy of vector to map to build index (API restriction) */
    map<int,Geometry*> geom_polygons;
    geom_polygons.clear();
    
    for (size_t j = 0; j < size; j++) {
      geom_polygons[j] = poly_set[j];
    }
    
    /* build spatial index for input polygons from idx2 */
    bool ret = build_index(geom_polygons);
    if (ret == false) {
      cout << "Error building index" << endl;
      return false;
    }
    return true;
  }

  /*
   * Compute bucket level similarity coefficient
   * Simple Jaccard and Dice are calculated per pair
   * This function calculates Dice similarity coefficient for whole bucket
   */
  double get_bucket_level_dice(){
    if (len1 <= 0 || len2 <= 0)
      return -1;

    Geometry* temp_polygon;
    Geometry* big_polygon1 = poly_set_one[0]->clone();
    Geometry* big_polygon2 = poly_set_two[0]->clone();
    
    for (size_t i=1; i<len1; ++i){
      temp_polygon = big_polygon1->Union(poly_set_one[0]);
      delete big_polygon1;
      big_polygon1 = temp_polygon;
    }
    for (size_t i=1; i<len2; ++i){
      temp_polygon = big_polygon2->Union(poly_set_two[0]);
      delete big_polygon2;
      big_polygon2 = temp_polygon;
    }

    double big_union_area = big_polygon1->Union(big_polygon2)->getArea();
    double big_intersection_area = big_polygon1->intersection(big_polygon2)->getArea();

    double dice = -1;
    if (big_union_area){
      double j = big_intersection_area/big_union_area;
      dice = 2.0*j/(1.0+j);
    }

    delete big_polygon1;
    delete big_polygon2;

    return dice;
  }
  
  ~Bucket(){
    delete spidx;
    delete storage;
  }
  
private:
  /* 
   * Create an R-Tree index on given set of polygons
   */
  bool build_index(map<int,Geometry*> & geom_polygons) {
    /* build spatial index on tile boundaries */
    id_type  indexIdentifier;
    
    GEOSDataStream stream(&geom_polygons);
    storage = StorageManager::createNewMemoryStorageManager();
    
    spidx   = RTree::createAndBulkLoadNewRTree(RTree::BLM_STR,
					       stream,
					       *storage, 
					       FillFactor,
					       IndexCapacity,
					       LeafCapacity,
					       2, 
					       RTree::RV_RSTAR,
					       indexIdentifier);
    
    /* Error checking */
    return spidx->isIndexValid();
  }
  
};
#endif
