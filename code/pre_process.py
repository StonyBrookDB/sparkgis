from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from shapely import wkt
from shapely.geometry import box
# local imports
import configs
from partitioner import Partitioner, FixedGridPartitioner

class Preprocess:
    def __init__(self, data1: DataFrame, data2: DataFrame):
        ''' Expects two datasets to preprocess for spatial join operations
        data1 (dataframe): Spark Dataframe with schema (id, geometry) for dataset 1
        data2 (dataframe): Spark Dataframe with schema (id, geometry) for dataset 2
        '''
        cols = ['gid', 'geometry']
        self.data1 = data1.toDF(*cols)
        self.data2 = data2.toDF(*cols)

        #self._partitioner = partitioner

        pass

    def _extract_mbbs(self, spark: SparkSession, data: DataFrame) -> Row:
        #####################################
        # Inner Functions
        #####################################
        def __sp_append_mbb(row: Row) -> Row:
            ''' (Processing: Spark RDD)
            Input - Row: (id, geometry)
            Output - Row(id, geometry, mbb_minx, mbb_miny, mbb_maxx, mbb_maxy)
            '''
            data = row.asDict()
            bounds = wkt.loads(data['geometry']).bounds

            data['mbb_minx'] = bounds[0]
            data['mbb_miny'] = bounds[1]
            data['mbb_maxx'] = bounds[2]
            data['mbb_maxy'] = bounds[3]

            return Row(**data)

        #####################################

        rdd_with_bb = data.rdd.map(__sp_append_mbb)
        data_with_mbb = spark.createDataFrame(rdd_with_bb)

        if configs.DEBUG:
            data_with_mbb.printSchema()
            data_with_mbb.show()

        return data_with_mbb

    def _determine_working_space(self, data1: DataFrame, data2: DataFrame):

        space1 = data1.agg(F.min(data1.mbb_minx).alias('minx'),
                           F.min(data1.mbb_miny).alias('miny'),
                           F.max(data1.mbb_maxx).alias('maxx'),
                           F.max(data1.mbb_maxy).alias('maxy')).first().asDict()

        space2 = data2.agg(F.min(data2.mbb_minx).alias('minx'),
                           F.min(data2.mbb_miny).alias('miny'),
                           F.max(data2.mbb_maxx).alias('maxx'),
                           F.max(data2.mbb_maxy).alias('maxy')).first().asDict()

        if configs.DEBUG:
            print(space1)
            print(space2)

        space = {}
        space['minx'] = min(space1['minx'], space2['minx'])
        space['miny'] = min(space1['miny'], space2['miny'])
        space['maxx'] = max(space1['maxx'], space2['maxx'])
        space['maxy'] = max(space1['maxy'], space2['maxy'])

        if configs.DEBUG:
            print(space)

        return space

    def _append_partition_id(self,
                             spark: SparkSession,
                             data: DataFrame,
                             partitioner: Partitioner,
                             rdd: bool = True) -> DataFrame:
        ''' On local setup, UDF based solution takes waaay longer than
        rdd based solution
        '''
        #####################################
        # Inner Functions
        #####################################
        def __sp_append_pid(mbb_minx: float, mbb_miny: float, mbb_maxx: float, mbb_maxy: float) -> int:
            query_polygon = box(mbb_minx, mbb_miny, mbb_maxx, mbb_maxy)
            p_idx = partitioner.partition_idx
            '''
            List of all partitions intersecting with this polygon i.e.
            Partition tile(s) this polygon belongs to.
            A polygon on partition boundary can belong to more than
            one partitions
            '''
            intersecting_partitions = p_idx.query(query_polygon)
            return partitioner.tiles_by_ids[id(intersecting_partitions[0])]

        def __sp_append_pid_rdd(row: Row) -> Row:
            data = row.asDict()
            query_polygon = box(data['mbb_minx'], data['mbb_miny'], data['mbb_maxx'], data['mbb_maxy'])
            p_idx = partitioner.partition_idx
            '''
            List of all partitions intersecting with this polygon i.e.
            Partition tile(s) this polygon belongs to.
            A polygon on partition boundary can belong to more than
            one partitions
            '''
            intersecting_partitions = p_idx.query(query_polygon)
            for ip in intersecting_partitions:
                data['partition'] = partitioner.tiles_by_ids[id(ip)]
                return Row(**data)

        if rdd:
            ##### RDD #####
            rdd_with_partition = data.rdd.map(__sp_append_pid_rdd)
            data_with_partition = spark.createDataFrame(rdd_with_partition)
        else:
            ##### UDF #####
            append_pid_udf = F.udf(__sp_append_pid, IntegerType())
            data_with_partition = data.withColumn('partition',
                                                  append_pid_udf('mbb_minx', 'mbb_miny', 'mbb_maxx', 'mbb_maxy'))

            pass

        ''' Since we need to groupby partition, we have to rearrange columns'''
        ret_data = data_with_partition.select('partition',
                                              'geometry',
                                              'mbb_minx',
                                              'mbb_miny',
                                              'mbb_maxx',
                                              'mbb_maxy')
        if configs.DEBUG:
            ret_data.show()

        return ret_data

    def execute(self, spark: SparkSession):
        ''' Extract MBBs for all polygons in all datasets '''
        self.data1 = self._extract_mbbs(spark, self.data1)
        self.data2 = self._extract_mbbs(spark, self.data2)

        ''' Determine working space using all polygons in all datasets '''
        data_space = self._determine_working_space(self.data1, self.data2)

        '''
        Further room for parallelization.
        Steps marked as I1 can be performed independently of eachother
        '''

        ''' I1 - Generate tiles using partitioning strategy '''
        partitioner = FixedGridPartitioner(data_space)
        ''' For metadata purposes only '''
        self.__partitioner_name = partitioner.__class__.__name__
        self.__space = data_space

        #self._partitioner.generate_partitions(data_space)

        ''' I1 - Append which dataset each polygon belongs to 
        Can be skipped since 'cogroup' will give two seperate lists
        of polygons corresponding to each dataset
        '''
        # self.data1 = self.data1.withColumn('set_num', F.lit(1))
        # self.data2 = self.data2.withColumn('set_num', F.lit(2))


        ''' Using partition index, determine which partition each polygon belongs to '''
        self.data1 = self._append_partition_id(spark, self.data1, partitioner)
        self.data2 = self._append_partition_id(spark, self.data2, partitioner)

        if configs.DEBUG:
            print('####################')
            # print(self.data1.select('partition').distinct().count())
            # print(self.data2.select('partition').distinct().count())

        pass

    def save(self, save_path: str):
        ''' metadata '''
        print('SparkGIS: Saving metadata: ' + save_path)
        meta_data = {}
        meta_data['dataset1_count'] = self.data1.count()
        meta_data['dataset2_count'] = self.data2.count()
        meta_data['partitioner'] = self.__partitioner_name
        meta_data['working_space'] = self.__space
        import json
        with open(save_path + '/metadata.json', 'w') as fp:
            json.dump(meta_data, fp)

        ''' preprocessed data '''
        print('SparkGIS: Saving preprocessed datasets: ' + save_path)
        if configs.DEBUG:
            self.data1.show()
            self.data2.show()
            
        # self.data1.write.parquet(save_path + '/dataset1/', mode='overwrite')
        # self.data2.write.parquet(save_path + '/dataset2/', mode='overwrite')

        # self.data1.write.csv(save_path + '/dataset1/', mode='overwrite')
        # self.data2.write.csv(save_path + '/dataset2/', mode='overwrite')

        # self.data1.rdd.saveAsTextFile(save_path + '/dataset1_rdd/')

        pass
