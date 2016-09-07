#include <string>
#include <cstring>
#include <cstdlib>

#include "../include/gis.hpp"
#include "../include/partitionMapperJoin.hpp"

void PartitionMapperJoin::doQuery(Geometry* poly) {
    double low[2], high[2];
    const Envelope * env = poly->getEnvelopeInternal();

    low [0] = env->getMinX();
    low [1] = env->getMinY();

    high [0] = env->getMaxX();
    high [1] = env->getMaxY();

    Region r(low, high, 2);
    // calling intersect
    spidx->intersectsWithQuery(r, vis);

}

vector<string> PartitionMapperJoin::parse(string & line) {
    vector<string> tokens ;
    tokenize(line, tokens,TAB,true);
    return tokens;
}

/********************************* Modifed Code *******************************/

/*
 * Instead of emitting to stdout, create and return vector of string
 */
vector<string> PartitionMapperJoin::emitHits(Geometry* poly, string input_line, int join_idx) {
  vector<string> ret;
  stringstream ss;

  vector<id_type> hits = vis.get_hits();
  int size = hits.size();
  for (uint32_t i=0; i<size; i++) 
    {
      //cout << hits[i] << endl;
      ss << hits[i] << TAB << join_idx << TAB << input_line;
      ret.push_back(ss.str());
      ss.str(string());
    }
  return ret;
}

/*
 * Build and return spatial index (ISpatialIndex *) pointer 
 */
void PartitionMapperJoin::build_index() {
    // build spatial index on tile boundaries 
    id_type  indexIdentifier;
    GEOSDataStream stream(&geom_tiles);
    IStorageManager *storage = StorageManager::createNewMemoryStorageManager();
    spidx   = RTree::createAndBulkLoadNewRTree(RTree::BLM_STR, stream, *storage, 
	    FillFactor,
	    IndexCapacity,
	    LeafCapacity,
	    2, 
	    RTree::RV_RSTAR, indexIdentifier);

    if (spidx == NULL){
      cout << "Error building index" << endl;
      return;
    }
}
/*
 * Generate tile and add to grid
 */
void PartitionMapperJoin::gen_tile(string input_line) {

  stringstream ss;
  vector<string> fields;
  double min_x, min_y, max_x, max_y;
  id_type id;

  // orginal code
  fields = parse(input_line);
  min_x = std::stod(fields[1]);
  min_y = std::stod(fields[2]);
  max_x = std::stod(fields[3]);
  max_y = std::stod(fields[4]);
      
  ss << shapebegin << min_x << SPACE << min_y << COMMA
     << min_x << SPACE << max_y << COMMA
     << max_x << SPACE << max_y << COMMA
     << max_x << SPACE << min_y << COMMA
     << min_x << SPACE << min_y << shapeend;
  
  // cerr << ss.str() << endl;
  // No index change required here since this is processing partfile.idx
  id = std::strtoul(fields[0].c_str(), NULL, 0);
  geom_tiles[id]= wkt_reader->read(ss.str());
  id_tiles[id] = id;
  
}

/*
 * Interface function called from JNI
 * idx_ptr: Spatial Index pointer created by build_index() - called from gis.cpp prior to this
 */
vector<string> PartitionMapperJoin::map_line(string input_line){

  vector<string> fields;
  id_type id = 0; 
  Geometry* geom ; 
  int join_idx = -1;

  // parse input line and create a geometry
  fields = parse(input_line);
  // fields[0] has the setNumber assigned while Reformating
  join_idx = (fields[0] == "1") ? 2:1;
  if (join_idx < 0) {
        cerr << "Invalid join index" << endl;
        // return empty vector
  	return vector<string>();
   }
  geom = wkt_reader->read(fields[geom_id]);
  // use index to map this geometry to generated tile(s)
  doQuery(geom);
  return emitHits(geom, input_line, join_idx);
  //return vector<string>();
}

PartitionMapperJoin::PartitionMapperJoin(int geomid){

  // do geomid shifting implicitly from original data geomid
  // setNumber & ID appended before calling this. Geomid shifted right by 2
  geom_id = geomid+2;

  gf = new GeometryFactory(new PrecisionModel(),0);
  wkt_reader = new WKTReader(gf);  
}
