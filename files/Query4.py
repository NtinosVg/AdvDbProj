from pyspark.sql import SparkSession
from pyspark.sql.functions import count,round,mean,asc,desc,year,col,udf
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType


spark = SparkSession \
    .builder \
    .appName("DF Query 4 Execution") \
    .getOrCreate()

spark.sparkContext.addPyFile("hdfs://okeanos-master:54310/user/user/dataset/geopy.zip")

import geopy.distance
#calculate the distance between two points [lat1,long1],[lat2,long2] in km.
def get_distance(lat1,lon1,lat2,lon2):
    return geopy.distance.geodesic((lat1,lon1),(lat2,lon2)).km


#crime schema creation
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

#lapd schema creation
lapd_schema = StructType([
    StructField("X", DoubleType()),
    StructField("Y", DoubleType()),
    StructField("FID", IntegerType()),
    StructField("DIVISION", StringType()),
    StructField("LOCATION", StringType()),
    StructField("PREC", IntegerType())
])

crime_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/crimedf/crime.csv")

lapd_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(lapd_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/la-police-stations")


get_distance_udf = udf(get_distance, DoubleType())

crime_df=crime_df.filter((crime_df.LAT!=0) & (crime_df.LON!=0)) \
    .filter(col("Weapon Used Cd").startswith("1"))  \
    .withColumn("year",year("Date Rptd")) \
    .drop("Date Rptd")

#print(crime_df.dropDuplicates(["AREA"]).count())
result_df=crime_df.join(lapd_df,crime_df.AREA.cast('int')==lapd_df.PREC,'inner')
result_df=result_df.withColumn("distance",get_distance_udf(col("LAT"),col("LON"),col("Y"),col("X")))

result1_df=result_df.groupBy("year").agg(mean("distance"),count("DR_NO")) \
.withColumn("distance",round("avg(distance)",3)) \
.drop("avg(distance)") \
.withColumn("crime_total",col("count(DR_NO)")) \
.drop("count(DR_NO)") \
.sort(asc("year"))
result1_df.show(30)

result2_df=result_df.groupBy("DIVISION").agg(mean("distance"),count("DR_NO")) \
.withColumn("distance",round("avg(distance)",3)) \
.drop("avg(distance)") \
.withColumn("crime_total",col("count(DR_NO)")) \
.withColumnRenamed("DIVISION","division") \
.drop("count(DR_NO)") \
.sort(desc("crime_total"))
result2_df.show(30)
