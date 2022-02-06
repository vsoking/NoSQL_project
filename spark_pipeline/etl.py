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
def unzipFile(zip_file):
    ret = None
    myzip = ZipFile(io.BytesIO(zip_file))
    #enc = chardet.detect(myzip.read(myzip.namelist()[0]))['encoding']
    ret = myzip.read(myzip.namelist()[0]).decode("utf-8")
    myzip.close()
    return ret

def questionOne(spark, event, mention):
    
    def getLang(col):
        return split(split(col, ':')[1], ';')[0]
    
    eventColIndx = [0, 1, 53]
    mentionColIndx = [0, 14]
    eventRDD = spark.sparkContext.parallelize(event).map(lambda x: [x.split('\t')[i] for i in eventColIndx])
    mentionRDD = spark.sparkContext.parallelize(mention).map(lambda x: [x.split('\t')[i] for i in mentionColIndx])
    #eventDF = spark.read.option("delimiter", ',').csv(eventRDD, header=False)
    eventDF = spark.createDataFrame(eventRDD, ["GlobalEventID","day", "country"])
    mentionDF = spark.createDataFrame(mentionRDD, ["GlobalEventID","sourcelanguage"])
    resultDF = eventDF.join(mentionDF, eventDF.GlobalEventID == mentionDF.GlobalEventID, "outer")\
                        .drop(mentionDF.GlobalEventID)\
                        .drop(eventDF.GlobalEventID)\
                        .withColumn("SourceLanguage", getLang(col("SourceLanguage")))\
                        .na.drop()\
                        .groupBy(["day","country","sourcelanguage"])\
                        .count()
    #eventDF.show()
    resultDF.show()
    resultDF.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode("append")\
            .options(table="table2", keyspace="test")\
            .save()

if __name__ == "__main__":
    conf = SparkConf().setAppName("Streaming_ETL_GDELT")\
            .set('spark.cassandra.connection.host', 'localhost')
            #.set("spark.dynamicAllocation.enabled", "true")\
            #.set("spark.shuffle.service.enabled", "true")
    spark = SparkSession\
            .builder\
            .config(conf=conf)\
            .getOrCreate()
    
    #eventSchema = gdeltschema.eventSchema
    master_file_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
    master_file = requests.get(master_file_url)
    url = "http:\/\/data.gdeltproject.org\/gdeltv2\/2021{:02d}.+"
    filelist_2021 = re.findall("http:\/\/data.gdeltproject.org\/gdeltv2\/2021.+", master_file.text)
    beginDate = date(2021, 1, 1)
    endDate = date(2021, 12, 31)
    delta = timedelta(days=1)
    for i in range(0,len(filelist_2021),3):
        filelist_2021_str = "\n ".join(filelist_2021[i:i+3])
        mention_url = re.findall(".+\.mentions\.CSV\.zip", filelist_2021_str)[0]
        export_url = re.findall(".+\.export\.CSV\.zip", filelist_2021_str)[0]
        gkg_url = re.findall(".+\.gkg\.csv\.zip", filelist_2021_str)[0]
        #print(mention_url, export_url, gkg_url)
        mention_csv = unzipFile(requests.get(mention_url).content).splitlines()
        event_csv = unzipFile(requests.get(export_url).content).splitlines()
        gkg_csv = unzipFile(requests.get(gkg_url).content).splitlines()
        questionOne(spark, event_csv, mention_csv)
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
