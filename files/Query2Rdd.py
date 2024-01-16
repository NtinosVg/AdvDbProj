from pyspark.sql import SparkSession

sc = SparkSession \
    .builder \
    .appName("Query 2 RDD") \
    .getOrCreate() \
    .sparkContext

def timeconversion(x):
    if (x>= 500 and x <= 1159):
        return ('morning',1)
    elif (x>=1200 and x<=1659):
        return('afternoon',1)
    elif (x>=1700 and x<=2059):
        return('night',1)
    elif(x>=2100 and x<=2359) or (x>=0 and x<=459):
        return ('midnight',1)
    else:
        return('bad-data',1)

crime = sc.textFile("hdfs://okeanos-master:54310/user/user/dataset/crimedf/crimerdd.csv") \
    .map(lambda x: (x.split("++"))) \
    .map(lambda x:x if(x[13]=="STREET") else None) \
    .filter(lambda x:x!=None) \
    .map(lambda x:int(x[1])) \
    .map(timeconversion) \
    .reduceByKey(lambda x,y:x+y) \
    .sortBy(lambda x:x[1],ascending=False)

print(crime.take(4))
print(crime.count())