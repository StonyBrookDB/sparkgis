#ifndef RTREE_H
#define RTREE_H

/* geos */
#include <geos/geom/Geometry.h>
/* libspatialindex */
#include <spatialindex/SpatialIndex.h>

/* 
 * The program maps the input tsv data into corresponding partition 
 * (it adds the prefix partition id number at the beginning of the line)
 * */

static SpatialIndex::RTree::Data* parseInputPolygon(geos::geom::Geometry *p,
						    SpatialIndex::id_type m_id) {
  double low[2], high[2];
  const geos::geom::Envelope * env = p->getEnvelopeInternal();
  low [0] = env->getMinX();
  low [1] = env->getMinY();

  high [0] = env->getMaxX();
  high [1] = env->getMaxY();

  SpatialIndex::Region r(low, high, 2);

  return new SpatialIndex::RTree::Data(0, 0 , r, m_id);// store a zero size null poiter.
}

class GEOSDataStream : public SpatialIndex::IDataStream
{
public:
  GEOSDataStream(map<int,geos::geom::Geometry*> * inputColl ) : m_pNext(0), len(0),m_id(0)
  {
    if (inputColl->empty())
      throw Tools::IllegalArgumentException("Input size is ZERO.");
    shapes = inputColl;
    len = inputColl->size();
    iter = shapes->begin();
    readNextEntry();
  }
  virtual ~GEOSDataStream()
  {
    if (m_pNext != 0) delete m_pNext;
  }

  virtual SpatialIndex::IData* getNext()
  {
    if (m_pNext == 0) return 0;
    
    SpatialIndex::RTree::Data* ret = m_pNext;
    m_pNext = 0;
    readNextEntry();
    return ret;
  }
  
  virtual bool hasNext()
  {
    return (m_pNext != 0);
  }

  virtual uint32_t size()
  {
    return len;
    //throw Tools::NotSupportedException("Operation not supported.");
  }

  virtual void rewind()
  {
    if (m_pNext != 0)
      {
	delete m_pNext;
	m_pNext = 0;
      }

    m_id  = 0;
    iter = shapes->begin();
    readNextEntry();
  }

  void readNextEntry()
  {
    if (iter != shapes->end())
      {
	//std::cerr<< "readNextEntry m_id == " << m_id << std::endl;
	m_id = iter->first;
	m_pNext = parseInputPolygon(iter->second, m_id);
	iter++;
      }
  }

  SpatialIndex::RTree::Data* m_pNext;
  map<int,geos::geom::Geometry*> * shapes; 
  map<int,geos::geom::Geometry*>::iterator iter; 

  int len;
  SpatialIndex::id_type m_id;
};

class MyVisitor : public SpatialIndex::IVisitor
{
private:
  vector<SpatialIndex::id_type> hits;
public:
  // BAIG WAS HERE
  vector<SpatialIndex::id_type> get_hits() {return hits;}
  void clear_hits(){hits.clear();}
  // BAIG ENDS HERE
  void visitNode(const SpatialIndex::INode& n) {}
  void visitData(std::string &s) {}

  void visitData(const SpatialIndex::IData& d)
  {
    hits.push_back(d.getIdentifier());
    //std::cout << d.getIdentifier()<< std::endl;
  }

  void visitData(std::vector<const SpatialIndex::IData*>& v) {}
  void visitData(std::vector<uint32_t>& v){}
};

#endif
