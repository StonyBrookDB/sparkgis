#ifndef GIS_H
#define GIS_H

#include <cmath>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <map>
#include <cstring>
#include <string>
#include <vector>
#include <list>

#define FillFactor 0.9
#define IndexCapacity 10 
#define LeafCapacity 50
#define COMPRESS true

using namespace std;

const string BAR = "|";
const string DASH= "-";
const string TAB = "\t";
const string COMMA = ",";
const string SPACE = " ";
const string SEP = "\t";

const string shapebegin = "POLYGON((";
const string shapeend = "))";

const int OSM_SRID = 4326;
const int ST_INTERSECTS = 1;
const int ST_TOUCHES = 2;
const int ST_CROSSES = 3;
const int ST_CONTAINS = 4;
const int ST_ADJACENT = 5;
const int ST_DISJOINT = 6;
const int ST_EQUALS = 7;
const int ST_DWITHIN = 8;
const int ST_WITHIN = 9;
const int ST_OVERLAPS = 10;
const int ST_NEAREST = 11;
const int ST_NEAREST_2 = 12;

const int SID_1 = 1;
const int SID_2 = 2;

/* General Utility functions */
namespace Util{
  void tokenize ( const string& str,
		  vector<string>& result,
		  const string& delimiters = " ,;:\t", 
		  const bool keepBlankFields=false,
		  const string& quote="\"\'"
		  );
}

struct query_op { 
  // int shape_idx_1;
  // int shape_idx_2;
  int shape_idx[2];
  int join_cardinality;
  double expansion_distance;
  vector<int> proj1; /* Output fields for 1st set  */
  vector<int> proj2; /* Output fields for 2nd set */

  int join_predicate;
  size_t k_neighbors; /* Number of neighbors in kNN */
};

/* placeholder for nearest neighbor ranking */
struct query_nn_dist{
  int object_id;
  double distance;
};

/* reusable temp variables in bucket processing */
struct bucket_temp{
  /* temp value placeholders for MBBs */
  double low[2];
  double high[2];
  std::list<struct query_nn_dist*> nearest_distances;
  union{
    struct{
      /* for kNN */
      double min_x;
      double min_y;
      double max_x;
      double max_y;
      double distance;
    }knn;
    struct{
      /* for spatial join statistics */
      double area1;
      double area2;
      double union_area;
      double intersection_area;
      double dice;
      double jaccard;
    }spj;
  };
};

#endif
