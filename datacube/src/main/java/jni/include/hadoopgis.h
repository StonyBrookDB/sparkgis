#include <cmath>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <map>

// libspatialindex
#include <spatialindex/SpatialIndex.h>

// geos 
#include <geos/geom/PrecisionModel.h>
#include <geos/geom/GeometryFactory.h>
#include <geos/geom/Geometry.h>
#include <geos/geom/Point.h>
#include <geos/io/WKTReader.h>

#include "tokenizer.h"

#define FillFactor 0.9
#define IndexCapacity 10 
#define LeafCapacity 50
#define COMPRESS true


using namespace SpatialIndex;

using namespace geos;
using namespace geos::io;
using namespace geos::geom;


const string BAR = "|";
const string DASH= "-";
const string TAB = "\t";
const string COMMA = ",";
const string SPACE = " ";

const string shapebegin = "POLYGON((";
const string shapeend = "))";
