import pandas as pd
import pyspark
import random
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructField,IntegerType, StructType,StringType, FloatType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
import pyspark.sql.functions as F
import numpy as np
from pyspark.sql import Window
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.types import DateType
from pyspark.sql.functions import *

# record path for example revenue data
path = 'Snowball example inputs v2.csv' # defline the file path

# define columns of interest
columns = ['Customer_ID', 'Product_ID', 'Month', 'IsRecurring', 'MRR']
# Pandas
df = pd.read_csv(path, usecols=columns)
# Pyspark
appName = "PySpark SQL Server Example - via JDBC"
master = "local"
conf = pyspark.SparkConf().set('spark.driver.host','127.0.0.1').setAppName(appName).setMaster(master).set("spark.driver.extraClassPath","sqljdbc_7.2/enu/mssql-jdbc-7.2.1.jre8.jar")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession.builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

# # fix data types for columns of interest in revenue data
newDF = [StructField('Customer_ID', IntegerType(), True), StructType('Product_ID', StringType(), True),
                 StructType('Month', DateType(), True), StructType('MRR', FloatType(), True),
                 StructType('IsRecurring', IntegerType(), True), StructType()]
finalStruct = StructType(fields=schema_fields)

# import spark dataframe
df_s = spark.read.csv(path, header=True)


# select columns of interest
df_s_new = []


# pandas
print(df.dtypes)

# pyspark
print(df_s.printSchema())