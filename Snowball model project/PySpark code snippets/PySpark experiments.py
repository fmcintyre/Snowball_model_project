from clean_data_columns_pyspark import clean_data_columns_pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

spark = SparkSession.builder.master("local").appName('SparkSessionTest').getOrCreate()

# record path for example revenue data
path = '..\Snowball model Pandas\Alteryx datasets\Snowball example inputs v2.csv'

# import spark dataframe
revenue_data_spark = spark.read.csv(path, header=True)

# define list of date column titles
date_columns = ['Month']

revenue_data_clean = clean_data_columns_pyspark(dataset=revenue_data_spark, date_columns=date_columns)

revenue_data_clean.show()




