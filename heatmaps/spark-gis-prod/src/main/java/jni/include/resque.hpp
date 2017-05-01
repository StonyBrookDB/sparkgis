#ifndef RESQUE_H
#define RESQUE_H

/* geos */
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
/* for kNN */
#include <geos/operation/distance/DistanceOp.h>

#include "gis.hpp"
#include "bucket.hpp"

using namespace SpatialIndex;
using namespace geos;
using namespace geos::io;
using namespace geos::geom;

class Resque{
  
public:
  Resque(int predicate, int geomid1, int geomid2);
  Resque(int predicate, int k, int geomid1, int geomid2);
  // Refer to JNIWrapper.java for documentation of following functions
  void populate(string input_line); 
  vector<string> join_bucket_spjoin();
  vector<string> join_bucket_knn();
  double tile_dice();
  
  ~Resque();
  
  // friend void swap(Resque& first, Resque& second) // nothrow
  // {
  //   // enable ADL (not necessary in our case, but good practice)
  //   using std::swap; 

  //   // by swapping the members of two classes,
  //   // the two classes are effectively swapped
  //   swap(first.tile_id, second.tile_id); 
  //   swap(first.polydata, second.polydata); 
  //   swap(first.rawdata, second.rawdata); 
  //   swap(first.wkt_reader, second.wkt_reader); 
  //   swap(first.jacc_cal, second.jacc_cal); 
  //   swap(first.dice_cal, second.dice_cal); 
  //   swap(first.appendstats, second.appendstats); 
  //   swap(first.appendTileID, second.appendTileID); 
  //   swap(first.area1, second.area1); 
  //   swap(first.area2, second.area2); 
  //   swap(first.stat_report, second.stat_report); 
  // }
  // Resque& operator=(Resque other)
  // {
  //   swap(*this, other);
  //   return *this;
  // }
  
private:
  string tile_id = "";
  struct query_op st_op;
  struct bucket_temp b_tmp;

  map<int, vector<Geometry*> > polydata;
  map<int, vector<string> > rawdata;
  
  WKTReader *wkt_reader;
  
  bool appendstats = false;
  bool appendTileID = false;

  string report_result(int i, int j);
  void release_shape_mem(const int k);
  void set_projection_param(char * arg);
  string project( vector<string> & fields, int sid);  
  void init_query_op(int predicate, int geomid1, int geomid2);
  void populate_polygon(Geometry *poly, int sid, vector<string> fields);
  

  /* nearest neighbor helper function: update nearest neighbor values */
  void update_nn(int object_id, double distance);
  
  /* spatial join helper function: for fine grained joining on filtered spatial objects */
  bool join_with_predicate(const Geometry * geom1 ,
			   const Geometry * geom2, 
			   const Envelope * env1,
			   const Envelope * env2,
			   const int jp);

};

#endif
