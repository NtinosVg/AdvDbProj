from pyspark.sql import SparkSession
from pyspark.sql.functions import month,year,col,row_number
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.window import Window


spark = SparkSession \
    .builder \
    .appName("Query 1 Dataframe") \
    .getOrCreate()

#CSV Schema
crime_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("TIME OCC", StringType()),
    StructField("AREA", StringType()),
    StructField("AREA NAM", StringType()),
    StructField("Rpt Dist No", StringType()),
    StructField("Part 1-2", StringType()),
    StructField("Crm Cd", StringType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", IntegerType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", StringType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", StringType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", StringType()),
    StructField("Crm Cd 2", StringType()),
    StructField("Crm Cd 3", StringType()),
    StructField("Crm Cd 4", StringType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
    StructField("Date Rptd", DateType()),
    StructField("Date OCC", DateType()),
])


crimedf = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/crimedf/crime.csv")

crimedf=crimedf.groupBy(year("Date Rptd"),month("Date Rptd")) \
.count() \
.withColumnRenamed("year(Date Rptd)","year") \
.withColumnRenamed("month(Date Rptd)","month") \
.withColumnRenamed("count","crime_total") 


window = Window.partitionBy(crimedf["year"]) \
.orderBy(crimedf["crime_total"].desc(),crimedf["month"].asc())
crimedf.withColumn('rank',row_number().over(window)) \
.filter(col('rank')<=3). \
show(10000,truncate=False)
