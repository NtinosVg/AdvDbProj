from pyspark.sql import SparkSession
from pyspark.sql.functions import substring,length,expr,asc,desc,regexp_replace,col
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, DateType

spark = SparkSession \
    .builder \
    .appName("Query 3 Dataframe 2 executors") \
    .getOrCreate()

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

# Geocode schema creation
geocode_schema = StructType([
    StructField("lat1", DoubleType()),
    StructField("lon1", DoubleType()),
    StructField("ZIPcode", StringType()),
])

# Income schema creation
income_schema = StructType([
    StructField("ZIP", StringType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType()),
])

crime_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/crimedf/crime.csv")

geocode_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(geocode_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/revgecoding.csv")

income_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(income_schema) \
    .load("hdfs://okeanos-master:54310/user/user/dataset/income/LA_income_2015.csv")


income_df=income_df.withColumn("inc",expr('substring(`Estimated Median Income`,2,length(`Estimated Median Income`)-1)')) \
.drop("Estimated Median Income") \
.withColumn("inc1",regexp_replace("inc",r'[,]','.')) \
.drop("inc") \
.withColumn("Income",col("inc1").cast('double')) \
.drop("inc1") 



crime_df=crime_df.filter(col("Vict Descent").isNotNull())

incomela_df=geocode_df.join(income_df,[(income_df.ZIP == geocode_df.ZIPcode)],'inner')

Lazips_df=incomela_df.dropDuplicates(["ZIPcode"]).select("ZIPCode","Income")
income1_df=Lazips_df.sort(asc("Income")).limit(3)
income2_df=Lazips_df.sort(desc("Income")).limit(3)
Inc_df=income1_df.union(income2_df)

incomela_df=incomela_df.join(Inc_df,[(income_df.ZIP == Inc_df.ZIPCode)],'inner').select("lat1","lon1","ZIP")

result_df=crime_df.join(incomela_df,[(crime_df.LON==incomela_df.lon1) & (crime_df.LAT==incomela_df.lat1)],'inner')
result_df=result_df.select("DR_NO","Vict Descent","ZIP")


#result_df.groupBy("DR_NO").count().where(col("count")==2).show(1000000)

#result_df=result_df.dropDuplicates(["DR_NO"]).select("Vict Descent")
result_df=result_df.groupBy("Vict Descent").count()
mapping = {
    'A': 'Other Asian',
    'B': 'Black',
    'C': 'Chinese',
    'D': 'Cambodian',
    'F': 'Filipino',
    'G': 'Guamanian',
    'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
    'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    'Z': 'Asian Indiana',
}

mapping_schema=StructType([
    StructField("Vict Descent", StringType()),
    StructField("Victim Descent", StringType()),
])
print(result_df.count())
mapping_df=spark.createDataFrame(mapping.items(),schema=mapping_schema)
result_df =result_df.join(mapping_df,'Vict Descent','left') \
.select("Victim Descent","count") \
.withColumnRenamed("count","Crimes") \
.sort(desc("Crimes"))
result_df.show(19,truncate=False)