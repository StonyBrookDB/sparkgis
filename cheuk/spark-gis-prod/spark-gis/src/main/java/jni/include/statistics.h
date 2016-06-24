#ifndef _STATISTICS_H_
#define _STATISTICS_H_

#include <geos/geom/Geometry.h>
using namespace geos::geom;

class Stat_cal 
{
public:
  virtual double calculate(std::vector<const Geometry *> &geom1, std::vector<const Geometry *> &geom2) = 0;
};


class Jacc_object_cal : public Stat_cal
{
public:
  virtual double calculate(std::vector<const Geometry *> &geom1, std::vector<const Geometry *> &geom2) {
     if (!geom1.size() || !geom2.size())  return -1;
     
     Geometry* geomUni = geom1[0]->Union(geom2[0]);
     double union_area = geomUni->getArea();
     Geometry* geomIntersect = geom1[0]->intersection(geom2[0]);
     double intersect_area = geomIntersect->getArea();
    
     double J = -1; 
     if (union_area)
       J = intersect_area / union_area;

     delete geomUni;
     delete geomIntersect;
     return J;
  }
};


class Dice_object_cal : public Stat_cal
{
public:
  virtual double calculate(std::vector<const Geometry *> &geom1, std::vector<const Geometry *> &geom2) {
    if (!geom1.size() || !geom2.size())  return -1;
     
    Geometry* geomUni = geom1[0]->Union(geom2[0]);
    double union_area = geomUni->getArea();
    Geometry* geomIntersect = geom1[0]->intersection(geom2[0]);
    double intersect_area = geomIntersect->getArea();
    
    double D = -1; 
    if (union_area) {
      double J = intersect_area / union_area;
      D = 2.0*J/(1.0 + J);
    }
    delete geomUni;
    delete geomIntersect;
    return D;
  }
};

class Dice_tile_cal : public Stat_cal
{
public:
  virtual double calculate(std::vector<const Geometry *> &geom1, std::vector<const Geometry *> &geom2) {
    if (!geom1.size() || !geom2.size()) {
      return -1;
    }

    Geometry* tmpGeometry;
    Geometry* bigPolygon1 = geom1[0]->clone();
    for (std::size_t it = 1; it < geom1.size(); ++it ) {
      tmpGeometry = bigPolygon1->Union(geom1[it]);
      delete bigPolygon1;
      bigPolygon1 = tmpGeometry;
    }

    Geometry* bigPolygon2 = geom2[0]->clone();
    for (std::size_t it = 1; it < geom2.size(); ++it ) {
      tmpGeometry = bigPolygon2->Union(geom2[it]);
      delete bigPolygon2;
      bigPolygon2 = tmpGeometry;
    }
    
    //std::cout << bigPolygon1->getGeometryType() << std::endl;
    //std::cout << bigPolygon2->getGeometryType() << std::endl;
    Geometry* geomUni = bigPolygon1->Union(bigPolygon2);
    double union_area = geomUni->getArea();

    Geometry* geomIntersect = bigPolygon1->intersection(bigPolygon2);
    double intersect_area = geomIntersect->getArea();

    double D = -1;
    if (union_area) {
      double J = intersect_area / union_area;
      D = 2.0*J/(1.0 + J);
    }
    delete geomUni;
    delete geomIntersect;
    delete bigPolygon1;
    delete bigPolygon2;

    return D;
  }
};

#endif
