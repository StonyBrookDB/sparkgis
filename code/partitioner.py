from math import ceil, floor
from shapely.geometry import box
from shapely.strtree import STRtree
from abc import ABC, abstractmethod
from typing import Dict

class Partitioner(ABC):
    def __init__(self, space: Dict[str, float]):
        ''' Abstract Base Class for generating partition tiles.
        Simply implement _generate_partitions() abstract method for any concrete partitioner

        :param dict space: Dictionary with bounds of working space
        '''
        self._space = space
        #self._partition_idx = None # shapely.strtree.STRtree
        self._partition_tiles = []
        self.tiles_by_ids = {} # dict of {shapely.geometry.box: id}
        pass

    @abstractmethod
    def _generate_partitions(self):
        pass

    @property
    def partition_idx(self) -> STRtree:
        if not self._partition_tiles:
            #raise ValueError('SparkGIS Partitioner: Empty partition tiles')
            self._generate_partitions()
        return STRtree(self._partition_tiles)

class FixedGridPartitioner(Partitioner):
    def __init__(self, space: Dict[str, float], partition_size: int = 32):
        '''
        :param dict space: Dictionary with bounds of working space
        :param int partition_size: Partition grid cell size hint. Actual size may be
        adjusted based on working space bounds while generating partitions
        '''
        super().__init__(space)
        self.__partition_size = partition_size
        pass

    def _generate_partitions(self):
        ''' ToDo: Corner case
        if (maxx - minx) < partition_size OR if (maxy - miny) < partition_size
        '''
        span_x = (self._space['maxx'] - self._space['minx'])
        span_y = (self._space['maxy'] - self._space['miny'])

        step_x = span_x if span_x <= self.__partition_size else span_x / self.__partition_size-1
        step_y = span_y if span_y <= self.__partition_size else span_y / self.__partition_size-1

        max_x = int(ceil(self._space['maxx']))
        max_y = int(ceil(self._space['maxy']))

        # ToDo: Validation required???
        step_x = int(ceil(step_x))
        step_y = int(ceil(step_y))
        min_x = int(floor(self._space['minx']))
        min_y = int(floor(self._space['miny']))
        #################

        for i in range(min_x, max_x, step_x):
            for j in range(min_y, max_y, step_y):
                # shapely.geometry.box(minx, miny, maxx, maxy, ccw=True)
                self._partition_tiles.append(box(i, j, (i+step_x), (j+step_y)))

        self.tiles_by_ids = dict((id(pt), i) for i, pt in enumerate(self._partition_tiles))
        pass


class BinarySpacePartitioner(Partitioner):
    def __init__(self, space: Dict[str, float]):
        raise NotImplementedError

    def _generate_partitions(self):
        raise NotImplementedError
