#include <vector>
#include <string>

#include "gis.hpp"

class PartitionMapperJoin{
  
private:
  ISpatialIndex *spidx;
  // GeometryFactory *gf = NULL;
  // WKTReader *wkt_reader = NULL;

  GeometryFactory *gf;
  WKTReader *wkt_reader;
  
  map<id_type,string> id_tiles ;
  map<int,Geometry*> geom_tiles;
  int geom_id;
  
  MyVisitor vis;

  vector<string> parse(string & line);
  void doQuery(Geometry* poly);
  vector<string> emitHits(Geometry* poly, string input_line, int join_idx);

public:
  PartitionMapperJoin(int geomid);
vector<string> map_line(std::string input_line);
  void gen_tile(std::string input_line);
  void build_index();
  //void partitionMapperJoin_free_mem(long spidx);
};
