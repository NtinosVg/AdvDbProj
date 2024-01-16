from pyspark.sql import SparkSession
from pyspark.sql.functions import split,to_date,to_timestamp,col
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType

spark = SparkSession \
    .builder \
    .appName("Dataframe Creation") \
    .getOrCreate()

# The schema of the CSV FILES
crime_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()),
    StructField("Date OCC", StringType()),
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
])

#Read first csv file
crime2010_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/crime-data-from-2010-to-2019")

#Read second csv file
crime2020_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/crime-data-from-2020-to-present")

#Union of the 2 dataframes
crime_df=crime2010_df.union(crime2020_df)

crime_df=crime_df.withColumnRenamed("Date Rptd","Date Rptd Temp").withColumnRenamed("Date OCC","Date OCC Temp")

#Change Type of 'Date Rptd' and 'Date OCC'
crime_df=crime_df.withColumn("Date Rptd",to_date(to_timestamp(col("Date Rptd Temp"),"MM/dd/yyyy hh:mm:ss a"))) \
.drop("Date Rptd Temp"). \
withColumn("Date OCC",to_date(to_timestamp(col("Date OCC Temp"),"MM/dd/yyyy hh:mm:ss a"))) \
.drop("Date OCC Temp") 

crime_df=crime_df.distinct()
#header='False',delimiter='++' for rdd queries
crime_df.coalesce(1).write.options(header='True',delimiter=',').csv("hdfs://okeanos-master:54310/user/user/dataset/crimedf")
