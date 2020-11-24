#!/usr/bin/env python

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql import functions
import os
import argparse


parser = argparse.ArgumentParser('Aggregate Hourly Interval AMI Data')
parser.add_argument('--input', dest='input', action='store', required=True)
parser.add_argument('--name', dest='name', action='store', required=True)
args = parser.parse_args()

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
    .load(args.input)

# Format Data
#============================================
use_dat = use_dat.withColumn('hour', functions.hour(use_dat.date))

# Aggregate
#============================================
use_dat = use_dat.groupBy('meterID', 'hour').avg('use')

# Export
#============================================
if not os.path.exists('output'):
    os.mkdir('output')

use_dat.repartition(1).write.csv(f'output/{args.name}', mode='overwrite', header=True)
