#ifndef RESQUE_H
#define RESQUE_H

#include "gis.hpp"
#include "statistics.hpp"
					     
class Resque{
  
public:
  Resque(string predicate, int geomid1, int geomid2); 
  // Refer to JNIWrapper.java for documentation of following functions
  void populate(string input_line); 
  vector<string> join_bucket_spjoin();
  double tile_dice();
  
  ~Resque();
  
  friend void swap(Resque& first, Resque& second) // nothrow
  {
    // enable ADL (not necessary in our case, but good practice)
    using std::swap; 

    // by swapping the members of two classes,
    // the two classes are effectively swapped
    swap(first.tile_id, second.tile_id); 
    swap(first.prev_id, second.prev_id); 
    swap(first.polydata, second.polydata); 
    swap(first.rawdata, second.rawdata); 
    swap(first.wkt_reader, second.wkt_reader); 
    swap(first.jacc_cal, second.jacc_cal); 
    swap(first.dice_cal, second.dice_cal); 
    swap(first.appendstats, second.appendstats); 
    swap(first.appendTileID, second.appendTileID); 
    swap(first.area1, second.area1); 
    swap(first.area2, second.area2); 
    swap(first.stat_report, second.stat_report); 
  }
  Resque& operator=(Resque other)
  {
    swap(*this, other);
    return *this;
  }
  
private:

  struct query_op { 
    int JOIN_PREDICATE;
    int shape_idx_1;
    int shape_idx_2;
    int join_cardinality;
    double expansion_distance;
    vector<int> proj1; /* Output fields for 1st set  */
    vector<int> proj2; /* Output fields for 2nd set */
  } stop; // st operator

  string tile_id = "";

  map<int, vector<Geometry*> > polydata;
  map<int, vector<string> > rawdata;
  
  WKTReader *wkt_reader;
  //WKTReader wkt_reader;
  Jacc_object_cal * jacc_cal;
  Dice_object_cal * dice_cal;
  
  bool appendstats = false;
  bool appendTileID = false;
  double area1 = -1;
  double area2 = -1;
  vector<double> stat_report;
  
  void init();
  void print_stop();
  string report_result(int i, int j);
  void release_shape_mem(const int k);
  void set_projection_param(char * arg);
  string project( vector<string> & fields, int sid);
  int get_join_predicate(const char * predicate_str);
  void populate_polygon(Geometry *poly, int sid, vector<string> fields);

   
  bool build_index(map<int,Geometry*> & geom_polygons,
		   ISpatialIndex* & spidx,
		   IStorageManager* & storage);
  
  bool join_with_predicate(const Geometry * geom1 ,
			   const Geometry * geom2, 
			   const Envelope * env1,
			   const Envelope * env2,
			   const int jp);

};
#endif
