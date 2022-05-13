from pyspark.sql import SparkSession
# local imports
import configs
from pre_process import Preprocess
from spatial_query import SpatialJoin
from post_process import plot_intersecting_polygons

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName('SparkGIS') \
        .getOrCreate()

    # ''' Read two input dataset'''
    data1 = spark.read.csv(configs.dataset_1, sep=configs.data_delim)
    data2 = spark.read.csv(configs.dataset_2, sep=configs.data_delim)

    if configs.DEBUG:
        data1 = data1.limit(1000)
        data2 = data2.limit(1000)

    if configs.DEBUG:
        print(data1.count())
        print(data2.count())

    p = Preprocess(data1, data2)
    p.execute(spark)
    #p.save(configs.output_dir)

    sp = SpatialJoin(p.data1, p.data2)
    intersecting_polyons = sp.execute()

    intersecting_polyons.saveAsTextFile(configs.output_dir)
    plot_intersecting_polygons(intersecting_polyons.take(100))
    
    # ''' Load saved data '''
    # data1 = spark.read.parquet(configs.output_dir + '/dataset1/')
    # data2 = spark.read.parquet(configs.output_dir + '/dataset2/')
    
    # if configs.DEBUG:
    #     print(data1.count())
    #     print(data2.count())

    spark.stop()
