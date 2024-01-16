from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType

spark = SparkSession \
    .builder \
    .appName("Query 1 SQL") \
    .getOrCreate()

#Schema of CSV File
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

#Read the new csv file that we created
crime_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/crimedf/crime.csv")

#Run the appropriate SQL queries
crime_df.createOrReplaceTempView("crime")

month1_year_query="SELECT EXTRACT(MONTH FROM `Date Rptd`) as month, EXTRACT(YEAR FROM `Date Rptd`) as year,  COUNT(DR_NO) AS crime_total \
FROM crime \
GROUP BY  \
  EXTRACT(MONTH FROM `Date Rptd`),\
  EXTRACT(YEAR FROM `Date Rptd`)"

f_query="SELECT year,month,crime_total,rownum as rank \
FROM (select month, year, crime_total, row_number() OVER (PARTITION BY year ORDER BY crime_total DESC) as rownum \
   FROM depb) as t \
WHERE t.rownum = 1  or t.rownum=2 or t.rownum=3 \
ORDER BY year ASC, crime_total DESC"

depB_id = spark.sql(month1_year_query)
depB_id.registerTempTable("depB")
final=spark.sql(f_query)

#Show the results
final.show(10000,truncate=False)
