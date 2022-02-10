from pyspark.sql import SparkSession
import threading
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
#import gdeltschema
import re
import requests
from zipfile import ZipFile
import io
import chardet
from pyspark.sql.functions import *
from datetime import date, timedelta
import sys

def unzipFile(zip_file):
    ret = None
    myzip = ZipFile(io.BytesIO(zip_file))
    #enc = chardet.detect(myzip.read(myzip.namelist()[0]))['encoding']
    try:
        ret = myzip.read(myzip.namelist()[0]).decode("iso-8859-1")
    except UnicodeDecodeError:
        enc = chardet.detect(myzip.read(myzip.namelist()[0]))['encoding']
        ret = myzip.read(myzip.namelist()[0]).decode(enc)
    myzip.close()
    return ret

class Question:

    def __init__(self, spark, indxCol):
        self.spark = spark
        self.colIndx = indxCol
        self.rdd = spark.sparkContext.parallelize([])
    def addData(self, data):
        tmp_rdd = spark.sparkContext.parallelize(data).map(lambda x: [x.split('\t')[i] for i in self.colIndx])
        #self.rdd = self.rdd.union(tmp_rdd)
    def write(self):
        pass


def questionOne(spark, event, mention):
    
    def getLang(col):
        return split(split(col, ':')[1], ';')[0]
    
    eventColIndx = [0, 1, 33, 53]
    mentionColIndx = [0, 14]
    eventRDD = spark.sparkContext.parallelize(event).map(lambda x: [x.split('\t')[i] for i in eventColIndx])    
    mentionRDD = spark.sparkContext.parallelize(mention).map(lambda x: [x.split('\t')[i] for i in mentionColIndx])
    eventDF = spark.createDataFrame(eventRDD, ["globaleventid","day", "numarticles", "actiongeocountrycode"])
    mentionDF = spark.createDataFrame(mentionRDD, ["globaleventid","mentiondoctranslationinfo"])\
                    .withColumn("mentiondoctranslationinfo", when(col("mentiondoctranslationinfo")=='', "eng").otherwise(getLang(col("mentiondoctranslationinfo"))))
    resultDF = eventDF.join(mentionDF, eventDF.globaleventid == mentionDF.globaleventid, "outer")\
                        .drop(mentionDF.globaleventid)\
                        .withColumn('globaleventid',col("globaleventid").cast("Integer"))\
                        .withColumn('day',col("day").cast("Integer"))\
                        .na.drop(subset="day")
    #mentionDF.show()
    #eventDF.show()
    #resultDF.show()
    resultDF.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .options(table="requete_1", keyspace="test")\
            .save()


def questionTwo (spark, event, mention):
    eventColIndx = [0, 1, 53]
    mentionColIndx = [0]
    
    eventRDD = spark.sparkContext.parallelize(event).map(lambda x: [x.split('\t')[i] for i in eventColIndx])
    mentionRDD = spark.sparkContext.parallelize(mention).map(lambda x: [x.split('\t')[i] for i in mentionColIndx])
    eventDF = spark.createDataFrame(eventRDD, ["globaleventid","day", "actiongeocountrycode"])\
                    .withColumn('globaleventid',col("globaleventid").cast("Integer"))\
                    .withColumn('day',to_date(col("day"), "yyyyMMdd"))\
                    .withColumn("year", year(col("day")))\
                    .withColumn("monthyear", month(col("day")))\
                    .withColumn("day", dayofweek(col("day")))
    mentionDF = spark.createDataFrame(mentionRDD, ["globaleventid"])\
                    .withColumn('globaleventid',col("globaleventid").cast("Integer"))\
                    .groupBy("globaleventid")\
                    .agg(count("globaleventid").alias("nummentions"))

    resultDF = eventDF.join(mentionDF, eventDF.globaleventid == mentionDF.globaleventid, "outer")\
                        .drop(mentionDF.globaleventid)\
                        .withColumn("actiongeocountrycode", when(col("actiongeocountrycode")=="" ,None).otherwise(col("actiongeocountrycode")))\
                        .na.drop(subset="actiongeocountrycode")

    eventDF.show()
    mentionDF.show()
    resultDF.show()
    resultDF.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .options(table="requete_2", keyspace="test")\
            .save()

#class QuestionThree(Question):

     

#    def write(self):

def questionThree(spark, gkg):
    gkgColIndx = [1, 3, 7, 9, 11, 15]

    gkgRDD = spark.sparkContext.parallelize(gkg)\
                    .map(lambda x: [x.split('\t')[i] for i in gkgColIndx])
    gkgDF = spark.createDataFrame(gkgRDD, ["date", "source", "themes", "lieux", "personnes", "tons"])\
                .withColumn("date", to_timestamp(col("date"), "yyyyMMddHHmmss"))\
                .withColumn("day", dayofweek(col("date")))\
                .withColumn("day", col("day").cast("Integer"))\
                .withColumn("month", month(col("date")))\
                .withColumn("month", col("month").cast("Integer"))\
                .withColumn("year", year(col("date")))\
                .withColumn("year", col("year").cast("Integer"))\
                .withColumn("themes", split(col("themes"), ';'))\
                .withColumn("themes", explode(col("themes")))\
                .withColumn("lieux", split(col("lieux"), ';'))\
                .withColumn("lieux", explode(col("lieux")))\
                .withColumn("lieux", split(col("lieux"), '#').getItem(2))\
                .withColumn("personnes", split(col("personnes"), ';'))\
                .withColumn("personnes", explode(col("personnes")))\
                .withColumn("tons", split(col("tons"), ',').getItem(0))\
                .withColumn("tons", col("tons").cast("float"))\
                .drop("date")\
                .na.drop(subset="source")
                #.groupBy(["themes", "personnes", "lieux"])\
                #.count()
                #.withColumnRenamed("count", "numarticles")
    gkgDF.show()
    gkgDF.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .options(table="requete_3", keyspace="test")\
            .save()

def questionFour(spark, event, mention, gkg):
    eventColIndx = [0, 1, 7, 17]
    mentionColIndx = [0, 3, 4]
    gkgColIndx = [2, 3, 7, 15]

    eventRDD = spark.sparkContext.parallelize(event).map(lambda x: [x.split('\t')[i] for i in eventColIndx])
    mentionRDD = spark.sparkContext.parallelize(mention).map(lambda x: [x.split('\t')[i] for i in mentionColIndx])
    gkgRDD = spark.sparkContext.parallelize(gkg).map(lambda x: [x.split('\t')[i] for i in gkgColIndx])

    eventDF = spark.createDataFrame(eventRDD, ["globaleventid", "date", "actor1countrycode", "actor2countrycode"])\
                    .withColumn("globaleventid", col("globaleventid").cast("Integer"))\
                    .withColumn("date", to_date(col("date"), "yyyyMMdd"))\
                    .withColumn("day", dayofweek(col("date")))\
                    .withColumn("day", col("day").cast("Integer"))\
                    .withColumn("month", month(col("date")))\
                    .withColumn("month", col("month").cast("Integer"))\
                    .withColumn("year", year(col("date")))\
                    .withColumn("year", col("year").cast("Integer"))\
                    .drop("date")
    mentionDF = spark.createDataFrame(mentionRDD, ["globaleventid","mentiontype", "mentionsourcename"])\
                    .withColumn("globaleventid", col("globaleventid").cast("Integer"))\
                    .withColumn("mentiontype", col("mentiontype").cast("Integer"))
    gkgDF = spark.createDataFrame(gkgRDD, ["sourcecollectionid","sourcecommonname", "themes", "tons"])\
                    .withColumn("sourcecollectionid", col("sourcecollectionid").cast("Integer"))\
                    .withColumn("tons", split(col("tons"), ',').getItem(0))\
                    .withColumn("tons", col("tons").cast("float"))
    resultDF = gkgDF.join(mentionDF, (gkgDF.sourcecollectionid == mentionDF.mentiontype) & (gkgDF.sourcecommonname == mentionDF.mentionsourcename), "inner")\
                    .join(eventDF, mentionDF.globaleventid == eventDF.globaleventid, "right")\
                    .drop("globaleventid",  "mentiontype", "mentionsourcename", "sourcecollectionid")\
                    .na.drop(subset=["actor1countrycode", "actor2countrycode"])

    resultDF.show()
    resultDF.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .options(table="requete_4", keyspace="test")\
            .save()


if __name__ == "__main__":
    conf = SparkConf().setAppName("Streaming_ETL_GDELT")\
            .set('spark.cassandra.connection.host', 'localhost')
            #.setMaster("local")
            #.set("spark.dynamicAllocation.enabled", "true")\
            #.set("spark.shuffle.service.enabled", "true")
    spark = SparkSession\
            .builder\
            .config(conf=conf)\
            .getOrCreate()
    
    #questionThree = QuestionThree(spark, [1, 3, 7, 9, 11, 15])
    master_translationfile_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
    master_file_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    master_file = requests.get(master_file_url)
    master_translationfile = requests.get(master_translationfile_url)
    urlRegex = "http:\/\/data.gdeltproject.org\/gdeltv2\/{}.+"
    

    mention =  []
    event = []
    gkg = []
    urlList = []
    day = date(2021, 1, 1)
    endDate = date(2021, 12, 31)
    period = timedelta(days=1)
    download = 0
    while(day <= endDate):
        daysList = [day + timedelta(days=x) for x in range(period.days)]
    
        for d in daysList:
            urlList += re.findall(urlRegex.format(d.strftime("%Y%m%d")), master_file.text)
    
        for url in urlList:
    
            file_ = unzipFile(requests.get(url).content).splitlines()
            
            if re.search(".+\.mentions\.CSV\.zip", url):
                #file_ = unzipFile(requests.get(url).content).splitlines()
                mention += file_
                download +=1
    
            elif re.search(".+\.export\.CSV\.zip", url):
                #file_ = unzipFile(requests.get(url).content).splitlines()
                event += file_
                download +=1
    
            elif re.search(".+\.gkg\.csv\.zip", url):
                gkg += file_
                #questionThree.addData(file_)
                download +=1
                break 
        print("\n Downloaded {} file size {} {}\n".format(download, sys.getsizeof(event), sys.getsizeof(mention)))
      
        
        questionOne(spark, event, mention)
        questionTwo(spark, event, mention)
        #questionThree.write()
        questionThree(spark, gkg)
        questionFour(spark, event, mention, gkg)
        event.clear()
        mention.clear()
        gkg.clear()
        day += period
        break
        
    spark.stop()
"""
    gdeltEventDF = spark\
            .readStream\
            .option("sep","\t")\
            .schema(eventSchema)\
            .csv("hdfs://tp-hadoop-29:8020/user/ubuntu/gdelt")

    gdeltEventDF.select('GlobalEventID', 'Day'\
            , 'MonthYear','Actor1Code')\
            .limit(3)

    query = gdeltEventDF\
            .writeStream\
            .outputMode("append")\
            .format("console")\
            .start()

    query.awaitTermination()
"""
