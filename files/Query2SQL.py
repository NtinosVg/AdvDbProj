from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType

spark = SparkSession \
    .builder \
    .appName("Query 2 SQL") \
    .getOrCreate()

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

crime_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/crimedf/crime.csv")

 #To utilize as SQL tables
crime_df.createOrReplaceTempView("crime")


help_table="SELECT `TIME OCC`, `Premis Desc`\
    FROM crime\
      WHERE `Premis Desc` = 'STREET';"
      
int_time="SELECT CAST(`TIME OCC`AS INT) as Time\
  FROM (SELECT `TIME OCC` \
  FROM Q2)"
    
last_query="SELECT CASE \
    WHEN (Time >= 500 AND Time <= 1159) THEN 'morning' \
    WHEN (Time >= 1200 AND Time <= 1659) THEN 'afternoon' \
    WHEN (Time >= 1700 AND Time <= 2059) THEN 'night' \
    WHEN (Time >= 2100 AND Time <= 2359) THEN 'midnight' \
    WHEN (Time >= 0 AND Time <= 459) THEN 'midnight' \
    ELSE 'NA' \
    END  as DayPart \
FROM Q3"

final_query= "SELECT DayPart, COUNT(DayPart) as crime_total \
FROM Q4 \
GROUP BY DayPart \
ORDER BY crime_total DESC;"

help_t = spark.sql(help_table)
help_t.createOrReplaceTempView("Q2")
time_t = spark.sql(int_time)
time_t.createOrReplaceTempView("Q3")
last_t = spark.sql(last_query)
last_t.createOrReplaceTempView("Q4")
final_query = spark.sql(final_query)
final_query.show()