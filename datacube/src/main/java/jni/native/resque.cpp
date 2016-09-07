#include <iostream>
#include "../include/resque.hpp"

// struct query_op { 
//   int JOIN_PREDICATE;
//   int shape_idx_1;
//   int shape_idx_2;
//   int join_cardinality;
//   double expansion_distance;
//   vector<int> proj1; /* Output fields for 1st set  */
//   vector<int> proj2; /* Output fields for 2nd set */
// } stop; // st operator 

void Resque::init(){
  // initlize query operator 
  stop.expansion_distance = 0.0;
  stop.JOIN_PREDICATE = 0;
  stop.shape_idx_1 = 0;
  stop.shape_idx_2 = 0 ;
  stop.join_cardinality = 0;
  // initlize statistics calculater
  jacc_cal = new Jacc_object_cal();
  dice_cal = new Dice_object_cal();
}

void Resque::print_stop(){
  // initlize query operator 
  std::cerr << "predicate: " << stop.JOIN_PREDICATE << std::endl;
  std::cerr << "distance: " << stop.expansion_distance << std::endl;
  std::cerr << "shape index 1: " << stop.shape_idx_1 << std::endl;
  std::cerr << "shape index 2: " << stop.shape_idx_2 << std::endl;
  std::cerr << "join cardinality: " << stop.join_cardinality << std::endl;
}


void Resque::releaseShapeMem(const int k ){
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

bool Resque::buildIndex(map<int,Geometry*> & geom_polygons) {
    // build spatial index on tile boundaries 
    id_type  indexIdentifier;
    GEOSDataStream stream(&geom_polygons);
    storage = StorageManager::createNewMemoryStorageManager();
    spidx   = RTree::createAndBulkLoadNewRTree(RTree::BLM_STR, stream, *storage, 
	    FillFactor,
	    IndexCapacity,
	    LeafCapacity,
	    2, 
	    RTree::RV_RSTAR, indexIdentifier);

    // Error checking 
    return spidx->isIndexValid();
}


bool Resque::join_with_predicate(const Geometry * geom1 , const Geometry * geom2, 
        const Envelope * env1, const Envelope * env2,
        const int jp){
  
  bool flag = false ; 
  BufferOp * buffer_op1 = NULL ;
  BufferOp * buffer_op2 = NULL ;
  Geometry* geom_buffer1 = NULL;
  Geometry* geom_buffer2 = NULL;
  Geometry* geomUni = NULL;
  Geometry* geomIntersect = NULL; 

  //jp = ST_INTERSECTS;

  switch (jp){

    case ST_INTERSECTS:
      flag = env1->intersects(env2) && geom1->intersects(geom2);
      if (flag && appendstats) {
             area1 = geom1->getArea();
             area2 = geom2->getArea();
             
             std::vector<const Geometry*> g1, g2;
             g1.push_back(geom1); 
             g2.push_back(geom2);
             stat_report.push_back(jacc_cal->calculate(g1,g2));
             stat_report.push_back(dice_cal->calculate(g1,g2));
      }
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

      geom_buffer1 = buffer_op1->getResultGeometry(stop.expansion_distance);
      
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
  return flag; 
}

/* Filter selected fields for output
 * If there is no field selected, output all fields (except tileid and joinid) */
string Resque::project( vector<string> & fields, int sid) {
  std::stringstream ss;
  switch (sid){
    case 1:
      if (stop.proj1.size() == 0) {
          /* Do not output tileid and joinid */
	  ss << fields[2];
          for (int i = 3 ; i < fields.size(); i++)
          {
             ss << TAB << fields[i];
          }
      } else {
          for (int i = 0 ; i <stop.proj1.size();i++)
          {
             if ( 0 == i )
               ss << fields[stop.proj1[i]] ;
             else
             {
                if (stop.proj1[i] < fields.size())
                ss << TAB << fields[stop.proj1[i]];
             }
          }
      }
      break;
    case 2:
       if (stop.proj2.size() == 0) {
          /* Do not output tileid and joinid */
	  ss << fields[2];
          for (int i = 3 ; i < fields.size(); i++)
          {
             ss << TAB << fields[i];
          }
      } else {
          for (int i = 0 ; i <stop.proj2.size();i++)
          {
             if ( 0 == i )
               ss << fields[stop.proj2[i]] ;
             else
             {
                if (stop.proj2[i] < fields.size())
                ss << TAB << fields[stop.proj2[i]];
             }
          }
      }
      break;
    default:
      break;
  }

  return ss.str();
}

/* Set output fields
 * Fields are "technically" off by 3 (2 from extra field 
 * and 1 because of counting from 1 ) 
 */
void Resque::setProjectionParam(char * arg)
{
  string param(arg);
  vector<string> fields;
  vector<string> selec;
  tokenize(param, fields,":");

  if (fields.size()>0)
  {
    tokenize(fields[0], selec,",");
    for (int i =0 ;i < selec.size(); i++)
      stop.proj1.push_back(atoi(selec[i].c_str()) + 2);
  }
  selec.clear();

  if (fields.size()>1)
  {
    tokenize(fields[1], selec,",");
    for (int i =0 ;i < selec.size(); i++)
      stop.proj2.push_back(atoi(selec[i].c_str()) + 2);
  }
}

int Resque::getJoinPredicate(const char * predicate_str)
{
  if (strcmp(predicate_str, "st_intersects") == 0) {
    // stop.JOIN_PREDICATE = ST_INTERSECTS;
    return ST_INTERSECTS ; 
  } 
  else if (strcmp(predicate_str, "st_touches") == 0) {
    return ST_TOUCHES;
  } 
  else if (strcmp(predicate_str, "st_crosses") == 0) {
    return ST_CROSSES;
  } 
  else if (strcmp(predicate_str, "st_contains") == 0) {
    return ST_CONTAINS;
  } 
  else if (strcmp(predicate_str, "st_adjacent") == 0) {
    return ST_ADJACENT;
  } 
  else if (strcmp(predicate_str, "st_disjoint") == 0) {
    return ST_DISJOINT;
  }
  else if (strcmp(predicate_str, "st_equals") == 0) {
    return ST_EQUALS;
  }
  else if (strcmp(predicate_str, "st_dwithin") == 0) {
    return ST_DWITHIN;
  }
  else if (strcmp(predicate_str, "st_within") == 0) {
    return ST_WITHIN;
  }
  else if (strcmp(predicate_str, "st_overlaps") == 0) {
    return ST_OVERLAPS;
  }
  else {
    // std::cerr << "unrecognized join predicate " << std::endl;
    return 0;
  }
}

/* 
 * BAIG WAS HERE
 * Report result separated by sep 
 */
string Resque::ReportResult( int i , int j)
{
  stringstream ss;
  
  switch (stop.join_cardinality){
  case 1:
    ss << rawdata[SID_1][i] << SEP << rawdata[SID_1][j] << endl;
    break;
  case 2:
    ss << rawdata[SID_1][i] << SEP << rawdata[SID_2][j]; 
    if (appendstats) {
      ss << SEP << area1 << TAB << area2;
      for ( int k = 0; k < stat_report.size(); ++k) ss << TAB << stat_report[k];
          stat_report.clear();
    }
    // BAIG WAS HERE: changed previd to tile_id 
    if (appendTileID) {
          ss << TAB << tile_id << endl; 
    }
    ss << endl;
    break;
  default:
    cout << "returning empty string" << endl;
    // return empty string
    return string();
  }
  return ss.str();
}

// BAIG WAS HERE: name changed from joinBucket()
double Resque::tile_dice(){

  bool selfjoin = stop.join_cardinality ==1 ? true : false ;
  int idx1 = SID_1 ; 
  int idx2 = selfjoin ? SID_1 : SID_2 ;
  std::vector<Geometry*>  & poly_set_one = polydata[idx1];
  std::vector<Geometry*>  & poly_set_two = polydata[idx2];
  
  int len1 = poly_set_one.size();
  int len2 = poly_set_two.size();
  if (len1 <= 0 || len2 <= 0) {
      return -1;
  }
  std::vector<const Geometry*> g1, g2;
  for (int i=0; i<len1; ++i){
    g1.push_back(poly_set_one[i]);
  }
  for (int i=0; i<len2; ++i){
    g2.push_back(poly_set_two[i]);
  }

  Dice_tile_cal *tile_dice_cal = new Dice_tile_cal();
  double tile_dice_result = tile_dice_cal->calculate(g1, g2);
  
  return tile_dice_result;
  
  //stringstream ss;
  //ss << tile_id << TAB << tile_dice_result << endl;
  //return ss.str();
}

vector<string> Resque::join_bucket() 
{
  int pairs = 0;
  bool selfjoin = stop.join_cardinality ==1 ? true : false ;
  int idx1 = SID_1 ; 
  int idx2 = selfjoin ? SID_1 : SID_2 ;
  double low[2], high[2];
  // return string vector
  vector<string> ret_vec;
  
  // for each tile (key) in the input stream 
  try { 

    std::vector<Geometry*>  & poly_set_one = polydata[idx1];
    std::vector<Geometry*>  & poly_set_two = polydata[idx2];
    
    int len1 = poly_set_one.size();
    int len2 = poly_set_two.size();

    if (len1 <= 0 || len2 <= 0) {
      //cout << "Length1:" << len1 << "Length2:" << len2 << endl;
      return vector<string>();
    }
    
    map<int,Geometry*> geom_polygons2;
    for (int j = 0; j < len2; j++) {
        geom_polygons2[j] = poly_set_two[j];
    }
    
    // build spatial index for input polygons from idx2
    bool ret = buildIndex(geom_polygons2);
    if (ret == false) {
      cout << "Error building index" << endl;
      return vector<string>();
    }
    
    for (int i = 0; i < len1; i++) {
        const Geometry* geom1 = poly_set_one[i];
        const Envelope * env1 = geom1->getEnvelopeInternal();
        low[0] = env1->getMinX();
        low[1] = env1->getMinY();
        high[0] = env1->getMaxX();
        high[1] = env1->getMaxY();
        /* Handle the buffer expansion for R-tree */
        if (stop.JOIN_PREDICATE == ST_DWITHIN) {
            low[0] -= stop.expansion_distance;
            low[1] -= stop.expansion_distance;
            high[0] += stop.expansion_distance;
            high[1] += stop.expansion_distance;
        }
        
        Region r(low, high, 2);
	//        hits.clear();
        MyVisitor vis;
        spidx->intersectsWithQuery(r, vis);
	vector<id_type> hits = vis.get_hits();

	//cout << "hits size: " << hits.size() << endl;
	
        for (uint32_t j = 0 ; j < hits.size(); j++ ) 
        {
	  if (hits[j] == i && selfjoin) {
                continue;
            }            
            const Geometry* geom2 = poly_set_two[hits[j]];
            const Envelope * env2 = geom2->getEnvelopeInternal();
	   
            if (join_with_predicate(geom1, geom2, env1, env2,
				    stop.JOIN_PREDICATE))  {
              //cout << "Got results" << endl;
	      // create a vector of strings to return
	      ret_vec.push_back(ReportResult(i,hits[j]));
	      pairs++;
            }
	    // else
	    //   cout << "No join result ..." << endl;
        }
    }
  } // end of try
  //catch (Tools::Exception& e) {
  catch (...) {
    cout << "******ERROR******" << endl;
    // return empty vector
    return vector<string>();
  } // end of catch
  // free memory
  releaseShapeMem(stop.join_cardinality);
  // return results
  return ret_vec;
}

/********************************* Modifed Code *******************************/

void Resque::populate(string input_line)
{
  string value;
  vector<string> fields;
  int sid = 0;

  Geometry *poly = NULL; 
  
  int tile_counter = 0;

  //  std::cerr << "Bucketinfo:[ID] |A|x|B|=|R|" <<std::endl;
  int index = -1; 
  
  tokenize(input_line, fields, TAB, true);
  sid = atoi(fields[1].c_str());
  tile_id = fields[0];  
  
  switch(sid){
  case SID_1:
    index = stop.shape_idx_1 ; 
    break;
  case SID_2:
    index = stop.shape_idx_2 ; 
    break;
  default:
    cout << "wrong sid : " << sid << endl;
    return;
  }

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
  // populate the bucket for join 
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


Resque::Resque(std::string predicate, int geomid1, int geomid2){
  init();
  wkt_reader = new WKTReader(new GeometryFactory(new PrecisionModel(),OSM_SRID));

  stop.JOIN_PREDICATE = getJoinPredicate(predicate.c_str());
  // do geomid shifting implicitly from original data geomid
  // tileID appended in addition to setNumber & ID appended before mapping 
  // Geomid shifted right by 2 i.e 4
  stop.shape_idx_1 = geomid1 + 2;
  stop.join_cardinality++;

  stop.shape_idx_2 = geomid2 + 2;
  stop.join_cardinality++;
  
  stop.expansion_distance = 0;
  // print all parametes
  setProjectionParam("");  
  appendstats = true;
  appendTileID = true; 

  // query operator validation 
  if (stop.JOIN_PREDICATE <= 0 )// is the predicate supported 
    { 
    cerr << "Query predicate is NOT set properly. Please refer to the documentation." << endl ; 
    return;
    }
  // if the distance is valid 
  if (ST_DWITHIN == stop.JOIN_PREDICATE && stop.expansion_distance == 0.0)
    { 
      cerr << "Distance parameter is NOT set properly. Please refer to the documentation." << endl ;
      return;
    }
  if (0 == stop.join_cardinality)
    {
      cerr << "Geometry field indexes are NOT set properly. Please refer to the documentation." << endl ;
      return; 
    }
  
  //print_stop();
}


Resque::~Resque(){
  // garbage collection
  // delete spidx;
  // delete storage;
  // delete jacc_cal;
  // delete dice_cal;
 
  // delete wkt_reader ;
}


// void Resque::populate_polygon(Geometry *poly, int sid, vector<string> fields)
// {
//   // populate the bucket for join 
//   polydata[sid].push_back(poly);
//   switch(sid){
//   case SID_1:
//     rawdata[sid].push_back(project(fields,SID_1));
//     break;
//   case SID_2:
//     rawdata[sid].push_back(project(fields,SID_2));
//     break;
//   default:
//     std::cerr << "wrong sid : " << sid << endl;
//     return;
//   }  
// }

// void Resque::populate_extra(){
//   if ( (extra_poly != NULL)){
//     populate_polygon(extra_poly, extra_sid, extra_fields);
//     extra_poly = NULL;
//     extra_sid = -1;
//   }
// }

// void Resque::reset(){
//   releaseShapeMem(stop.join_cardinality);
// }


// /*
//  * Returns true if this line's tileID is same as previous line's tileID
//  */
// bool Resque::populate2(string input_line)
// {
//   string value;
//   vector<string> fields;
//   int sid = 0;

//   Geometry *poly = NULL; 
  
//   int tile_counter = 0;

//   //  std::cerr << "Bucketinfo:[ID] |A|x|B|=|R|" <<std::endl;
//   int index = -1; 
  
//   //cout << input_line << endl;
  
//   tokenize(input_line, fields, TAB, true);
//   sid = atoi(fields[1].c_str());
//   tile_id = fields[0];  
  
//   switch(sid){
//   case SID_1:
//     index = stop.shape_idx_1 ; 
//     break;
//   case SID_2:
//     index = stop.shape_idx_2 ; 
//     break;
//   default:
//     cout << "wrong sid : " << sid << endl;
//     return false;
//   }

//   //cout << "Index: " << index << endl;
  
//   if (fields[index].size() < 4) // this number 4 is really arbitrary
//     return false; // empty spatial object 

//   try { 
//     poly = wkt_reader->read(fields[index]);
//   }
//   catch (...) {
//     cout << "******Geometry Parsing Error******" << std::endl;
//     return false;
//   }
  
//   /*
//    * Corner case
//    */
//   if (prev_id.compare(tile_id) == 0){
//     //populate_polygon(poly, sid, fields);
    
//     // populate the bucket for join 
//     polydata[sid].push_back(poly);
//     switch(sid){
//     case SID_1:
//       rawdata[sid].push_back(project(fields,SID_1));
//     break;
//     case SID_2:
//       rawdata[sid].push_back(project(fields,SID_2));
//       break;
//     default:
//       cout << "wrong sid : " << sid << endl;
//       return false;
//     }
    
//     prev_id = tile_id;
//     return true;
//   }
//   else{
//     cout << "New set. prev_id:" << prev_id << ", tile_id:" << tile_id << endl;
//     //extra_poly = poly;
//     //extra_sid = sid;
//     //extra_fields = fields;
//     prev_id = tile_id;
//     return false;
//   }
  
// }
