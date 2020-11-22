import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql import functions

# Import Data
#============================================
schema = StructType() \
    .add('meterID', StringType()) \
    .add('date', TimestampType()) \
    .add('use', DoubleType()) \
    .add('temp', DoubleType())

spark = SparkSession.builder \
    .master('local') \
    .appName('aggregate'). \
    getOrCreate()

use_dat = spark.read.format("csv") \
    .option("header", True) \
    .schema(schema) \
    .load('sample.csv')

# Format Data
#============================================
use_dat = use_dat.withColumn('hour', functions.hour(use_dat.date))

# Aggregate
#============================================
use_dat = use_dat.groupBy('meterID', 'hour').avg('use')

# Export
#============================================
use_dat.repartition(1).write.csv('output/hour.csv', mode='overwrite', header=True)
help(use_dat.write)
