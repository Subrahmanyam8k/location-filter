
from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.types import StructType,StructField,StringType,LongType
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import reverse_geocoder as reverse_geocoder
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,BooleanType
import sys
import logging
from helpers import s3
import datetime

from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

import boto3
from awsglue.transforms import *
from pyspark.context import SparkContext


# To filter mexico data based on lat and long
@F.udf(returnType=BooleanType())
def mexico_filter(latitude,longitude):
    print(" ##################################################### Enrty of the User Defind Method is############################ ",latitude,longitude)
    geo = reverse_geocoder.RGeocoder(mode=1, verbose=True, stream=obj_data)
    coordinates = (latitude, longitude),(latitude, longitude)
    results = geo.query(coordinates)
    for response_dictionary in results:
        if [ x for x in response_dictionary if response_dictionary[x] == 'MX']:
            #logger.info("#######################  MX RECORD  ######################################################")
            return True
    return False


# Creates spark session
spark = SparkSession \
            .builder \
            .appName("test-application") \
            .getOrCreate()

filtered_keys=['geo-intl/2018-10-27/data-00.csv.gz']
source_s3_bucket='whisper-geo-history'
target_s3_bucket='whisper-geo-archive-aleatica'


for key_path in filtered_keys:
    print(key_path)
    #whisper_geo_dataset_path = "s3a://"+source_s3_bucket+"/"+key_path+"*.csv.gz"
    whisper_geo_dataset_path = "s3a://"+source_s3_bucket+"/"+key_path
    
  
    whisper_geo_df = spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("sep", "|") \
        .load(whisper_geo_dataset_path)

    glueContext=GlueContext(SparkContext.getOrCreate())
    whisper_geo_df_dynamicFrame=DynamicFrame.fromDF(whisper_geo_df.limit(10),glueContext,"SparkContext_to_GlueContext")
    print(whisper_geo_df_dynamicFrame.printSchema())
    #mexico_filter_df=whisper_geo_df_dynamicFrame.filter(mexico_filter('_c1','_c2'))
    mexico_data = Filter.apply(frame = whisper_geo_df_dynamicFrame,f = lambda x: mexico_filter(x["_c1"],x["_c2"]))
    
    output_directory=key_path[key_path.find("/")+1:]
    output_path="s3a://"+target_s3_bucket+"/"+ output_directory

    glueContext.write_dynamic_frame.from_options(
          frame = mexico_data,
          connection_type = "s3",
          connection_options = {"path": output_path,'compression':'gzip'},
          format = "csv",
          format_options={'separator': '|'}
          ) 

    #mexico_filter_df.write.format("com.databricks.spark.csv").option("codec","org.apache.hadoop.io.compress.GzipCodec").option("sep", "|").mode("append").save(output_path)

print(" ############# Script Ended ################")
#logger.info("########################################################################################################################  EXIT THE PIPELINE  #######################################################################################################################################################################")
#except Exception as e:
#logger.exception('Exception occurs while filtering mexico geo data')






