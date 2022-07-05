# imports
import os

# spark imports
import findspark
findspark.init()

import pyspark
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

from pyspark.sql.functions import *
import pyspark.sql as pys
import pyspark.sql.functions as psf

import pyspark.ml as Pipeline
import pyspark.ml.param as pmparam 
import pyspark.ml.pipeline as pmpip 


# class SparkConnection:
    
# def __init__(**kwargs):
#     self.app_name = kwargs.get('app_name','App1')
#     self.master = kwargs.get('master','yarn')
#     self.ui_port = kwargs.get('ui_port','4044')
#     self.driver_port = kwargs.get('driver_port','8887')
#     self.cores_max = kwargs.get('cores_max','2')
#     self.executor_cores = kwargs.get('executor_cores','1')
#     self.driver_memory = kwargs.get('driver_memory','10g') 
#     self.executor_memory = kwargs.get('executor_memory','10g')
#     self.dynamicAllocation = kwargs.get('dynamicAllocation','false')
#     self.aqe = kwargs.get('aqe', 'true')
#     self.sql_shuffle_partitions = kwargs.get('sql_shuffle_partitions',1000)

# create spark connection
def createSparkConnection(**kwargs):
    app_name = kwargs.get('app_name','App1')
    master = kwargs.get('master','yarn')
    ui_port = kwargs.get('ui_port','4044')
    driver_port = kwargs.get('driver_port','8887')
    cores_max = kwargs.get('cores_max','4')
    executor_cores = kwargs.get('executor_cores','4')
    driver_memory = kwargs.get('driver_memory','10g') 
    executor_memory = kwargs.get('executor_memory','10g')
    dynamicAllocation = kwargs.get('dynamicAllocation','false')
    aqe = kwargs.get('aqe', 'true')
    sql_shuffle_partitions = kwargs.get('sql_shuffle_partitions',1000)

    # define python envs
    os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

    # setting up spark configurations 
    conf = SparkConf()  

    conf.setAppName(app_name)
    conf.set('spark.master', master)

    # setting spark ports
    conf.set('spark.ui.port', ui_port)
    conf.set('spark.driver.port', driver_port)

    # setting spark cores
    conf.set('spark.cores.max', cores_max)
    conf.set('spark.executor.cores', executor_cores)

    # setting spark memory
    conf.set('spark.driver.memory', driver_memory)
    conf.set('spark.executor.memory', executor_memory)

    # setting 
    conf.set("spark.dynamicAllocation.enabled", dynamicAllocation)

    # setting spark sql properties
    conf.set('spark.sql.shuffle.partitions', sql_shuffle_partitions)
    conf.set('spark.sql.adaptive.coalescePartitions.enabled', aqe)

    # building ipark session
    spark = SparkSession.builder \
    .config(conf=conf) \
    .enableHiveSupport() \
    .getOrCreate()

    # sc = spark.sparkContext
    # sc.setLogLevel('WARN')

    return spark

def closeSparkConnection(sc):
    sc.stop()
        
# if __name__ == "__main__":
#     c = SparkConnection()
#     c.createSparkConnection()