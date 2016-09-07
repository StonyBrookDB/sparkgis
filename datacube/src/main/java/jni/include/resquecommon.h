//#include <iostream>
//#include <cstring>
//#include <cmath>
//#include <map>
//#include <cstdlib> 
//#include <getopt.h>

// tokenizer 
//#include "tokenizer.h"

// geos
//#include <geos/geom/PrecisionModel.h>
//#include <geos/geom/GeometryFactory.h>
//#include <geos/geom/Geometry.h>
//#include <geos/geom/Point.h>
//#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include <geos/opBuffer.h>

//#include <spatialindex/SpatialIndex.h>

//using namespace geos;
//using namespace geos::io;
//using namespace geos::geom;
using namespace geos::operation::buffer; 

//using namespace SpatialIndex;

//#define FillFactor 0.9
//#define IndexCapacity 10 
//#define LeafCapacity 50
//#define COMPRESS true

// Constants
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

const int SID_1 = 1;
const int SID_2 = 2;

// separators for parsing
//const string TAB = "\t";
const string SEP = "\t"; // ctrl+a
//const string BAR = "|";
//const string DASH= "-";
//const string COMMA = ",";
//const string SPACE = " ";

//const string shapebegin = "POLYGON((";
//const string shapeend = "))";

//vector<id_type> hits;

//BAIG ????
//RTree::Data* parseInputPolygon(Geometry *p, id_type m_id);

/* class MyVisitor : public IVisitor */
/* { */
/*     public: */
/* 	void visitNode(const INode& n) {} */
/* 	void visitData(std::string &s) {} */

/* 	void visitData(const IData& d) */
/* 	{ */
/* 	    hits.push_back(d.getIdentifier()); */
/* 	    //std::cout << d.getIdentifier()<< std::endl; */
/* 	} */

/* 	void visitData(std::vector<const IData*>& v) {} */
/* 	void visitData(std::vector<uint32_t>& v){} */
/* }; */


/* class GEOSDataStream : public IDataStream */
/* { */
/*     public: */
/* 	GEOSDataStream(map<int,Geometry*> * inputColl ) : m_pNext(0), len(0),m_id(0) */
/*     { */
/* 	if (inputColl->empty()) */
/* 	    throw Tools::IllegalArgumentException("Input size is ZERO."); */
/* 	shapes = inputColl; */
/* 	len = inputColl->size(); */
/* 	iter = shapes->begin(); */
/* 	readNextEntry(); */
/*     } */
/* 	virtual ~GEOSDataStream() */
/* 	{ */
/* 	    if (m_pNext != 0) delete m_pNext; */
/* 	} */

/* 	virtual IData* getNext() */
/* 	{ */
/* 	    if (m_pNext == 0) return 0; */

/* 	    RTree::Data* ret = m_pNext; */
/* 	    m_pNext = 0; */
/* 	    readNextEntry(); */
/* 	    return ret; */
/* 	} */

/* 	virtual bool hasNext() */
/* 	{ */
/* 	    return (m_pNext != 0); */
/* 	} */

/* 	virtual uint32_t size() */
/* 	{ */
/* 	    return len; */
/* 	    //throw Tools::NotSupportedException("Operation not supported."); */
/* 	} */

/* 	virtual void rewind() */
/* 	{ */
/* 	    if (m_pNext != 0) */
/* 	    { */
/* 		delete m_pNext; */
/* 		m_pNext = 0; */
/* 	    } */

/* 	    m_id  = 0; */
/* 	    iter = shapes->begin(); */
/* 	    readNextEntry(); */
/* 	} */

/* 	void readNextEntry() */
/* 	{ */
/* 	    if (iter != shapes->end()) */
/* 	    { */
/* 		//std::cerr<< "readNextEntry m_id == " << m_id << std::endl; */
/* 		m_id = iter->first; */
/* 		m_pNext = parseInputPolygon(iter->second, m_id); */
/* 		iter++; */
/* 	    } */
/* 	} */

/* 	RTree::Data* m_pNext; */
/* 	map<int,Geometry*> * shapes;  */
/* 	map<int,Geometry*>::iterator iter;  */

/* 	int len; */
/* 	id_type m_id; */
/* }; */


/* RTree::Data* parseInputPolygon(Geometry *p, id_type m_id) { */
/*     double low[2], high[2]; */
/*     const Envelope * env = p->getEnvelopeInternal(); */
/*     low [0] = env->getMinX(); */
/*     low [1] = env->getMinY(); */

/*     high [0] = env->getMaxX(); */
/*     high [1] = env->getMaxY(); */

/*     Region r(low, high, 2); */

/*     return new RTree::Data(0, 0 , r, m_id);// store a zero size null poiter. */
/* } */

