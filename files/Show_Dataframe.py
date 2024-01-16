from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType

spark = SparkSession \
    .builder \
    .appName("Show Dataframe") \
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


crime_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/crimedf/crime.csv")


#Print dataframe info
print("Total rows: ", crime_df.count())
print("Column Types:")
for types in crime_df.dtypes:
    print(types[0],": " ,types[1])

crime_df.drop("Crm Cd 1","Crm Cd 2","Crm Cd 3","Crm Cd 4").show()
