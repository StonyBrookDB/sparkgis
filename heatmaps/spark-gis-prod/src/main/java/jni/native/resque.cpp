#include <iostream>
/* geos */
#include <geos/opBuffer.h>
#include <geos/geom/Point.h>
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/GeometryFactory.h>
/* local */
#include "../include/resque.hpp"

using namespace geos::operation::buffer;
using namespace geos::operation::distance;

void Resque::release_shape_mem(const int k ){
  if (k <=0)
    return ;
  for (int j =0 ; j <k ;j++ )
    {
      int delete_index = j+1 ;
      int len = polydata[delete_index].size();

      for (int i = 0; i < len ; i++)
	delete polydata[delete_index][i];

      polydata[delete_index].clear();
      rawdata[delete_index].clear();
    }
}

/*
 * Fine grained joining only for polygons whose MBBs overlap
 */
bool Resque::join_with_predicate(const Geometry * geom1,
				 const Geometry * geom2,
				 const Envelope * env1,
				 const Envelope * env2,
				 const int jp
				 ){
  /* predicate satisfied or not */
  bool flag = false ;
  BufferOp * buffer_op1 = NULL ;
  Geometry* geom_buffer1 = NULL;

  switch (jp){

  case ST_INTERSECTS:
    flag = env1->intersects(env2) && geom1->intersects(geom2);
    break;

  case ST_TOUCHES:
    flag = geom1->touches(geom2);
    break;

  case ST_CROSSES:
    flag = geom1->crosses(geom2);
    break;

  case ST_CONTAINS:
    flag = env1->contains(env2) && geom1->contains(geom2);
    break;

  case ST_ADJACENT:
    flag = ! geom1->disjoint(geom2);
    break;

  case ST_DISJOINT:
    flag = geom1->disjoint(geom2);
    break;

  case ST_EQUALS:
    flag = env1->equals(env2) && geom1->equals(geom2);
    break;

  case ST_DWITHIN:
    buffer_op1 = new BufferOp(geom1);

    if (NULL == buffer_op1)
      cerr << "NULL: buffer_op1" <<endl;

    geom_buffer1 = buffer_op1->getResultGeometry(st_op.expansion_distance);

    if (NULL == geom_buffer1)
      cerr << "NULL: geom_buffer1" <<endl;

    flag = join_with_predicate(geom_buffer1,geom2, env1, env2, ST_INTERSECTS);
    break;

  case ST_WITHIN:
    flag = geom1->within(geom2);
    break;

  case ST_OVERLAPS:
    flag = geom1->overlaps(geom2);
    break;

  default:
    std::cerr << "ERROR: unknown spatial predicate " << endl;
    break;
  }

  /* extra spatial computation */
  if (flag){
    b_tmp.spj.area1 = geom1->getArea();
    b_tmp.spj.area2 = geom2->getArea();

    b_tmp.spj.union_area = geom1->Union(geom2)->getArea();
    b_tmp.spj.intersection_area = geom1->intersection(geom2)->getArea();

    b_tmp.spj.dice = 2*b_tmp.spj.intersection_area/(b_tmp.spj.area1 + b_tmp.spj.area1);
    if (b_tmp.spj.union_area)
      b_tmp.spj.jaccard = b_tmp.spj.intersection_area/b_tmp.spj.union_area;

  }
  /* TODO: add parameters to toggle extra computation if not required*/
  // stat_report.push_back(b_tmp.spj.jaccard);
  // stat_report.push_back(b_tmp.spj.dice);

  return flag;
}

/*
 * Filter selected fields for output
 * If there is no field selected, output all fields (except tileid and joinid)
 */
string Resque::project( vector<string> & fields, int sid) {
  std::stringstream ss;
  switch (sid){
  case 1:
    if (st_op.proj1.size() == 0) {
      /* Do not output tileid and joinid */
      ss << fields[2];
      for (size_t i = 3 ; i < fields.size(); i++)
	{
	  ss << TAB << fields[i];
	}
    } else {
      for (size_t i = 0 ; i <st_op.proj1.size();i++)
	{
	  if ( 0 == i )
	    ss << fields[st_op.proj1[i]] ;
	  else
	    {
	      if (st_op.proj1[i] < fields.size())
                ss << TAB << fields[st_op.proj1[i]];
	    }
	}
    }
    break;
  case 2:
    if (st_op.proj2.size() == 0) {
      /* Do not output tileid and joinid */
      ss << fields[2];
      for (size_t i = 3 ; i < fields.size(); i++)
	{
	  ss << TAB << fields[i];
	}
    } else {
      for (size_t i = 0 ; i <st_op.proj2.size();i++)
	{
	  if ( 0 == i )
	    ss << fields[st_op.proj2[i]] ;
	  else
	    {
	      if (st_op.proj2[i] < fields.size())
                ss << TAB << fields[st_op.proj2[i]];
	    }
	}
    }
    break;
  default:
    break;
  }

  return ss.str();
}

/*
 * Set output fields
 * Fields are "technically" off by 3 (2 from extra field
 * and 1 because of counting from 1 )
 */
void Resque::set_projection_param(char * arg)
{
  string param(arg);
  vector<string> fields;
  vector<string> selec;
  Util::tokenize(param, fields,":");

  if (fields.size()>0)
    {
      Util::tokenize(fields[0], selec,",");
      for (size_t i =0 ;i < selec.size(); i++)
	st_op.proj1.push_back(atoi(selec[i].c_str()) + 2);
    }
  selec.clear();

  if (fields.size()>1)
    {
      Util::tokenize(fields[1], selec,",");
      for (size_t i =0 ;i < selec.size(); i++)
	st_op.proj2.push_back(atoi(selec[i].c_str()) + 2);
    }
}

/*
 * BAIG WAS HERE
 * Report result separated by sep
 */
string Resque::report_result( int i , int j)
{
  stringstream ss;

  switch (st_op.join_cardinality){
  case 1:
    ss << rawdata[SID_1][i] << SEP << rawdata[SID_1][j] << endl;
    break;
  case 2:
    ss << rawdata[SID_1][i] << SEP << rawdata[SID_2][j];
    if (appendstats) {
      ss << SEP << b_tmp.spj.area1 << TAB << b_tmp.spj.area2;
      //for ( size_t k = 0; k < stat_report.size(); ++k) ss << TAB << stat_report[k];
      //stat_report.clear();
      ss << TAB << b_tmp.spj.jaccard << TAB << b_tmp.spj.dice;
    }
    if (appendTileID) {
      ss << TAB << tile_id << endl;
    }
    ss << endl;
    break;
  default:
    cout << "returning empty string" << endl;
    /* return empty string */
    return string();
  }
  return ss.str();
}

/*
 * Perform tile level dice similarity coeffcient value
 */
double Resque::tile_dice(){

  Bucket b(st_op, polydata);
  return b.get_bucket_level_dice();
}

/* kNN helper functions */
void Resque::update_nn(int object_id, double distance){
  list<struct query_nn_dist*>::iterator it;
  bool new_inserted = false;
  struct query_nn_dist * tmp;

  for (it = b_tmp.nearest_distances.begin();
       it != b_tmp.nearest_distances.end(); it++) {
    if ((*it)->distance > distance) {
      /* Insert the new distance in */
      tmp = new struct query_nn_dist();
      tmp->distance = distance;
      tmp->object_id = object_id;
      b_tmp.nearest_distances.insert(it, tmp);
      new_inserted = true;
      break;
    }
  }

  if (b_tmp.nearest_distances.size() > st_op.k_neighbors) {
    /* the last element is the furthest from all tracked nearest neighbors */
    tmp = b_tmp.nearest_distances.back();
    b_tmp.nearest_distances.pop_back();
    delete tmp;
  } else if (!new_inserted &&
	     b_tmp.nearest_distances.size() < st_op.k_neighbors) {
    /* Insert at the end */
    tmp = new struct query_nn_dist();
    tmp->distance = distance;
    tmp->object_id = object_id;
    b_tmp.nearest_distances.insert(it, tmp);
  }

}

vector<string> Resque::join_bucket_knn()
{
  double tmp_distance;
  /* reset all temp values */
  b_tmp = {};
  /* for nearest neighbor with unknown bounds */
  double max_search_radius = -1;
  double def_search_radius = -1;

  Bucket b(st_op, polydata);

  /* either of the dataset is empty */
  if (b.len1 <= 0 || b.len2 <= 0) {
    return vector<string>();
  }

  /* for each tile (key) in the input stream */
  try {

    std::vector<Geometry*>  & poly_set_one = b.get_dataset(b.idx1);
    std::vector<Geometry*>  & poly_set_two = b.get_dataset(b.idx2);

    if (st_op.join_predicate == ST_NEAREST_2){
      /* update bucket information */
      if (b.len2 > 0){
	const Envelope *env_tmp = poly_set_two[0]->getEnvelopeInternal();
	b_tmp.knn.min_x = env_tmp->getMinX();
	b_tmp.knn.min_y = env_tmp->getMinY();
	b_tmp.knn.max_x = env_tmp->getMaxX();
	b_tmp.knn.max_y = env_tmp->getMaxY();
      }
      /* update bucket dimensions */
      for (size_t i=0; i < b.len1; ++i){
	const Envelope *env = poly_set_one[i]->getEnvelopeInternal();
	if (b_tmp.knn.min_x >= env->getMinX()){ b_tmp.knn.min_x = env->getMinX();}
	if (b_tmp.knn.min_y >= env->getMinY()){ b_tmp.knn.min_y = env->getMinY();}
	if (b_tmp.knn.max_x <= env->getMaxX()){ b_tmp.knn.max_x = env->getMaxX();}
	if (b_tmp.knn.max_y <= env->getMaxY()){ b_tmp.knn.max_y = env->getMaxY();}
      }
      if (!b.selfjoin){
	for (size_t i=0; i < b.len1; ++i){
	  const Envelope *env = poly_set_two[i]->getEnvelopeInternal();
	  if (b_tmp.knn.min_x >= env->getMinX()){ b_tmp.knn.min_x = env->getMinX();}
	  if (b_tmp.knn.min_y >= env->getMinY()){ b_tmp.knn.min_y = env->getMinY();}
	  if (b_tmp.knn.max_x <= env->getMaxX()){ b_tmp.knn.max_x = env->getMaxX();}
	  if (b_tmp.knn.max_y <= env->getMaxY()){ b_tmp.knn.max_y = env->getMaxY();}
	}
      }
      double span_x = b_tmp.knn.max_x - b_tmp.knn.min_x;
      double span_y = b_tmp.knn.max_y - b_tmp.knn.min_y;
      max_search_radius = max(span_x, span_y);
      def_search_radius = min(sqrt(span_x * span_y * st_op.k_neighbors/b.len2),
			      max_search_radius);
      if (def_search_radius == 0){
	def_search_radius = DistanceOp::distance(poly_set_one[0], poly_set_two[0]);
      }

    }

    /* build index on dataset 2 */
    if (!b.build_rtree_index(2))
      throw std::runtime_error("[resque.cpp] Error building index");

    for (size_t i = 0; i < b.len1; i++) {
      /* extract MBB */
      const Geometry* geom1 = poly_set_one[i];
      const Envelope * env1 = geom1->getEnvelopeInternal();

      b_tmp.low[0] = env1->getMinX();
      b_tmp.low[1] = env1->getMinY();
      b_tmp.high[0] = env1->getMaxX();
      b_tmp.high[1] = env1->getMaxY();
      /* Handle the buffer expansion for R-tree */
      if (st_op.join_predicate == ST_NEAREST_2) {
	/* initial value when max search radius is not known */
	st_op.expansion_distance = def_search_radius;
      }
      if (st_op.join_predicate == ST_DWITHIN || st_op.join_predicate == ST_NEAREST) {
	b_tmp.low[0] -= st_op.expansion_distance;
	b_tmp.low[1] -= st_op.expansion_distance;
	b_tmp.high[0] += st_op.expansion_distance;
	b_tmp.high[1] += st_op.expansion_distance;
      }

      MyVisitor vis;
      double search_radius = def_search_radius;
      /* retrive at least k neighbours */
      do{
	Region r(b_tmp.low, b_tmp.high, 2);
	vis.clear_hits();
	/* get a list of Polygon MBBs intersecting with current polygon MBB */
	b.spidx->intersectsWithQuery(r, vis);

	/* increase radius */
	b_tmp.low[0] = env1->getMinX() - search_radius;
	b_tmp.low[1] = env1->getMinY() - search_radius;
	b_tmp.high[0] = env1->getMaxX() + search_radius;
	b_tmp.high[1] = env1->getMaxY() + search_radius;

	search_radius *= sqrt(2);

      }while(st_op.join_predicate == ST_NEAREST_2 &&
	     vis.get_hits().size() <= st_op.k_neighbors+1 &&
	     vis.get_hits().size() <= b.len2 &&
	     search_radius <= sqrt(2) * max_search_radius);


      if (st_op.join_predicate == ST_NEAREST_2){
	/* handles special case of rectangular/circular expansion - sqrt(2) expansion */
	vis.clear_hits();
	b_tmp.low[0] = env1->getMinX() - search_radius;
	b_tmp.low[1] = env1->getMinY() - search_radius;
	b_tmp.high[0] = env1->getMaxX() + search_radius;
	b_tmp.high[1] = env1->getMaxY() + search_radius;
	Region r(b_tmp.low, b_tmp.high, 2);
	/* get a list of Polygon MBBs intersecting with current polygon MBB */
	b.spidx->intersectsWithQuery(r, vis);
      }

      vector<id_type> hits = vis.get_hits();

      for (size_t j=0; j<hits.size(); ++j){
	const Geometry* geom2 = poly_set_two[hits[j]];
	// const Envelope * env2 = geom2->getEnvelopeInternal();

	/* skip results seen before */
	if (st_op.join_predicate == ST_NEAREST &&
	    (!b.selfjoin || hits[j] != i)){

	  tmp_distance = DistanceOp::distance(geom1, geom2);
	  if (tmp_distance < st_op.expansion_distance){
	    update_nn(hits[j], tmp_distance);
	  }
	}
	else if (st_op.join_predicate == ST_NEAREST_2 &&
		 (!b.selfjoin || hits[j] != i)){
	  tmp_distance = DistanceOp::distance(geom1, geom2);
	  update_nn(hits[j], tmp_distance);
	}
      }

      /* report results - TODO: add to return vector */
      for (std::list<struct query_nn_dist*>::iterator it = b_tmp.nearest_distances.begin();
      	   it != b_tmp.nearest_distances.end();
      	   ++it){
      	b_tmp.knn.distance = (*it)->distance;
      	// TODO ...
	delete *it;
      }
      b_tmp.nearest_distances.clear();
    }
  }catch (Tools::Exception& e) {
    cout << "******ERROR******" << endl;
    std::cerr << e.what() << endl;
  } /* end of catch */
  /* free memory */
  release_shape_mem(st_op.join_cardinality);
  /* return results */
  return b.ret_vec;
}

vector<string> Resque::join_bucket_spjoin()
{
  Bucket b(st_op, polydata);
  /* reset all temp values */
  b_tmp = {};
  /* either of the dataset is empty */
  if (b.len1 <= 0 || b.len2 <= 0) {
    return vector<string>();
  }

  /* for each tile (key) in the input stream */
  try {

    std::vector<Geometry*>  & poly_set_one = b.get_dataset(b.idx1);
    std::vector<Geometry*>  & poly_set_two = b.get_dataset(b.idx2);
    /* build index on dataset 2 */
    if (!b.build_rtree_index(2))
      throw std::runtime_error("[resque.cpp] Error building index");

    for (size_t i = 0; i < b.len1; i++) {
      /* extract MBB */
      const Geometry* geom1 = poly_set_one[i];
      const Envelope * env1 = geom1->getEnvelopeInternal();

      b_tmp.low[0] = env1->getMinX();
      b_tmp.low[1] = env1->getMinY();
      b_tmp.high[0] = env1->getMaxX();
      b_tmp.high[1] = env1->getMaxY();
      /* Handle the buffer expansion for R-tree */
      if (st_op.join_predicate == ST_DWITHIN) {
	b_tmp.low[0] -= st_op.expansion_distance;
	b_tmp.low[1] -= st_op.expansion_distance;
	b_tmp.high[0] += st_op.expansion_distance;
	b_tmp.high[1] += st_op.expansion_distance;
      }
      /* Regular handling */
      Region r(b_tmp.low, b_tmp.high, 2);
      MyVisitor vis;
      vis.clear_hits();
      /* get a list of Polygon MBBs intersecting with current polygon MBB */
      b.spidx->intersectsWithQuery(r, vis);

      vector<id_type> hits = vis.get_hits();

      for (size_t j = 0 ; j < hits.size(); j++ ){
	/* skip results seen before */
	if (hits[j] == i && b.selfjoin) {
	  continue;
	}
	const Geometry* geom2 = poly_set_two[hits[j]];
	const Envelope * env2 = geom2->getEnvelopeInternal();
	
	/* Perform actual spatial join only for polygons whose MBBs overlap */
	if (join_with_predicate(geom1, geom2, env1, env2, st_op.join_predicate))  {
	  /* create a vector of strings to return */
	  b.ret_vec.push_back(report_result(i,hits[j]));
	}
      }
    }
  } /* end of try */
  catch (Tools::Exception& e) {
    cout << "******ERROR******" << endl;
    std::cerr << e.what() << endl;
  } /* end of catch */
  /* free memory */
  release_shape_mem(st_op.join_cardinality);
  /* return results */
  return b.ret_vec;
}

/*
 * Populate spatial data in bucket
 * Takes in raw string spatial data and converts that to local
 * data structures required for further processing
 */
void Resque::populate(string input_line)
{
  string value;
  vector<string> fields;
  int sid = 0;

  Geometry *poly = NULL;
  int index = -1;

  Util::tokenize(input_line, fields, TAB, true);
  sid = atoi(fields[1].c_str());
  tile_id = fields[0];

  if (sid != SID_1 && sid != SID_2){
    cout << "wrong sid : " << sid << endl;
    return;
  }
  index = st_op.shape_idx[sid-1];

  // switch(sid){
  // case SID_1:
  //   index = st_op.shape_idx_1 ;
  //   break;
  // case SID_2:
  //   index = st_op.shape_idx_2 ;
  //   break;
  // default:
  //   cout << "wrong sid : " << sid << endl;
  //   return;
  // }

  if (fields[index].size() < 4) // this number 4 is really arbitrary
    return; // empty spatial object

  try {
    poly = wkt_reader->read(fields[index]);
  }
  catch (...) {
    cout << "******Geometry Parsing Error******" << index << endl;
    cout << input_line << endl;
    return;
  }
  /* populate the bucket for join */
  polydata[sid].push_back(poly);
  switch(sid){
  case SID_1:
    rawdata[sid].push_back(project(fields,SID_1));
    break;
  case SID_2:
    rawdata[sid].push_back(project(fields,SID_2));
    break;
  default:
    cout << "wrong sid : " << sid << endl;
    return;
  }
}

void Resque::init_query_op(int predicate, int geomid1, int geomid2){
  /* initlize query operator */
  st_op = {};

  st_op.join_predicate = predicate; //get_join_predicate(predicate.c_str());

  // do geomid shifting implicitly from original data geomid
  // tileID appended in addition to setNumber & ID appended before mapping
  // Geomid shifted right by 2 i.e 4
  st_op.shape_idx[0] = geomid1 + 2;
  // st_op.shape_idx_1 = geomid1 + 2;
  st_op.join_cardinality++;

  st_op.shape_idx[1] = geomid2 + 2;
  // st_op.shape_idx_2 = geomid2 + 2;
  st_op.join_cardinality++;

  st_op.expansion_distance = 0;

  // query operator validation
  if (st_op.join_predicate <= 0 )// is the predicate supported
    {
      cerr << "Query predicate is NOT set properly. Please refer to the documentation." << endl ;
      return;
    }
  // if the distance is valid
  if (ST_DWITHIN == st_op.join_predicate && st_op.expansion_distance == 0.0)
    {
      cerr << "Distance parameter is NOT set properly. Please refer to the documentation." << endl ;
      return;
    }
  if (0 == st_op.join_cardinality)
    {
      cerr << "Geometry field indexes are NOT set properly. Please refer to the documentation." << endl ;
      return;
    }
}

/*
 * Constructor for spatial join
 */
Resque::Resque(int predicate, int geomid1, int geomid2){

  init_query_op(predicate, geomid1, geomid2);
  wkt_reader = new WKTReader(new GeometryFactory(new PrecisionModel(),OSM_SRID));

  // print all parametes
  set_projection_param("");
  appendstats = true;
  appendTileID = true;

}

/*
 * Constructor for kNN
 */
Resque::Resque(int predicate, int k, int geomid1, int geomid2){

  init_query_op(predicate, geomid1, geomid2);
  st_op.k_neighbors = k;

  wkt_reader = new WKTReader(new GeometryFactory(new PrecisionModel(),OSM_SRID));

  // print all parametes
  set_projection_param("");
  appendstats = true;
  appendTileID = true;

}


Resque::~Resque(){
  /* garbage collection */
  delete wkt_reader;
}
