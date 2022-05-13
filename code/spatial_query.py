from typing import Tuple, List
from pyspark import RDD
from pyspark.sql import Row, DataFrame
from shapely import wkt
from shapely.geometry import box
from shapely.strtree import STRtree
import configs

class SpatialJoin:
    def __init__(self, data1: DataFrame, data2: DataFrame):
        self.data1 = data1
        self.data2 = data2

        pass

    def execute(self) -> 'RDD[Tuple[Polygon, Polygon]]':
        def _sp_process_join(combined_datasets: Tuple[List, List]) -> List[Tuple['Polygon', 'Polygon']]:
            ''' create local index for dataset2 on every partition '''
            index_polygons = []
            for data in combined_datasets[1]:
                index_polygons.append(wkt.loads(data['geometry']))
            local_index = STRtree(index_polygons)

            ''' for every polygon in dataset1, get polygons from dataset2 whose bounding boxes intersect '''
            potentially_intersecting = []
            for data in combined_datasets[0]:
                poly_geom = wkt.loads(data['geometry'])
                intersecting_mbbs = local_index.query(poly_geom)

                for im in intersecting_mbbs:
                    potentially_intersecting.append((poly_geom.wkt, im.wkt))
                    pass
                pass

            ''' perform intersection check only for polygons whose bouding boxes intersect'''
            ret = potentially_intersecting
            return ret

            # ''' Count # of polygons in each partition '''
            # d1 = len(combined_datasets[0])
            # d2 = len(combined_datasets[1])
            # return (d1, d2)

        data1_rdd = self.data1.rdd.map(lambda x: (x.asDict()['partition'], x.asDict() ))
        data2_rdd = self.data2.rdd.map(lambda x: (x.asDict()['partition'], x.asDict() ))

        ''' For every partition, get all polygons from both sets and perform join opeartion '''
        result = data1_rdd.cogroup(data2_rdd).flatMapValues(_sp_process_join)
        return result.values()

class SpatialRange:
    pass

class SpatialkNN:
    pass
