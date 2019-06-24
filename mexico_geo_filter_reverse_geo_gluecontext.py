
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

from pyspark.context import SparkContext


logging.basicConfig()
logger = logging.getLogger()
#logger.setLevel(logging.DEBUG)

# To filter mexico data based on lat and long
@F.udf(returnType=BooleanType())
def mexico_filter(latitude,longitude):
    geo = reverse_geocoder.RGeocoder(mode=1, verbose=True, stream=obj_data)
    coordinates = (latitude, longitude),(latitude, longitude)
    results = geo.query(coordinates)
    for response_dictionary in results:
        if [ x for x in response_dictionary if response_dictionary[x] == 'MX']:
            #logger.info("#######################  MX RECORD  ######################################################")
            return True
    return False

start_time = datetime.datetime.now()

# Reading glue job parametrs
args = getResolvedOptions(sys.argv,['Mexico_Geo_Filter_V1_Source','Mexico_Geo_Filter_V1_Target','Archive_Start_Date','Archive_End_Date'])

# Represents source bucket
source_s3_bucket = args['Mexico_Geo_Filter_V1_Source']
# Represents target bucket
target_s3_bucket = args['Mexico_Geo_Filter_V1_Target']

filtered_keys = []
# Represents start date
input_start_date = args['Archive_Start_Date']
# Represents end date
input_end_date = args['Archive_End_Date']

# Creating S3 client
s3_client = boto3.resource('s3')

obj_data = s3.retrieve_object(s3_client,"mexico-geo-filter-reverse-geo-source-code","rg_cities1000.csv")
#try:
s3_client= boto3.client('s3')
all_s3_keys_list = s3.retrieve_objects_list(s3_client, source_s3_bucket,prefix="geo-intl/", delimiter="/")

# Creates spark session
spark = SparkSession \
            .builder \
            .appName("test-application") \
            .getOrCreate()

# broadcast_data = spark.sparkContext(obj_data)
print("#############################################################################################################################  PIPELINE ENTRY  ###################################################################################################################################################################")

archive_start_date = datetime.datetime.strptime(input_start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
archive_end_date = datetime.datetime.strptime(input_end_date, "%Y-%m-%d").strftime("%Y-%m-%d")

for s3_key in all_s3_keys_list:
    if not s3_key.get('Prefix'):
        break
    else:
        if str(s3_key['Prefix']).endswith("/"):
            s3_directory=s3_key['Prefix'][0:-1]
            s3_directory=s3_directory[s3_directory.find("/")+1:]
            s3_directory_date=datetime.datetime.strptime(s3_directory, "%Y-%m-%d").strftime("%Y-%m-%d")
            if (archive_start_date<=s3_directory_date) and (s3_directory_date<=archive_end_date):
                filtered_keys.append(s3_key['Prefix'])

if not filtered_keys:
    print("#############################################################################################################################  DATA NOT AVAILABLE IN GIVEN INTERVAL  ###########################################################################################################################################")    


print("Filtered Keys are ###################################################: ",filtered_keys)

for key_path in filtered_keys:
    print(key_path)
    whisper_geo_dataset_path = "s3a://"+source_s3_bucket+"/"+key_path+"*.csv.gz"
    whisper_geo_df = spark.read.format("csv") \
        .option("inferSchema", "true") \
        .option("sep", "|") \
        .load(whisper_geo_dataset_path)

    glueContext=GlueContext(SparkContext.getOrCreate())
    whisper_geo_df_dynamicFrame=DynamicFrame.fromDF(whisper_geo_df,glueContext,"SparkContext_to_GlueContext")
    mexico_filter_df=whisper_geo_df_dynamicFrame.filter(mexico_filter('_c1','_c2'))
    output_directory=key_path[key_path.find("/")+1:]
    output_path="s3a://"+target_s3_bucket+"/"+ output_directory

    glueContext.write_dynamic_frame.from_options(
          frame = mexico_filter_df,
          connection_type = "s3",
          connection_options = {"path": output_path},
          format = "csv",
          connection_options={'compression':'gzip'}
          format_options={'separator': '|'}
          ) 

    #mexico_filter_df.write.format("com.databricks.spark.csv").option("codec","org.apache.hadoop.io.compress.GzipCodec").option("sep", "|").mode("append").save(output_path)

end_time = datetime.datetime.now()
time_difference = end_time - start_time
print(" THE TOTAL EXECUTION TIME IS :::::::",time_difference.seconds)

#logger.info("########################################################################################################################  EXIT THE PIPELINE  #######################################################################################################################################################################")
#except Exception as e:
#logger.exception('Exception occurs while filtering mexico geo data')






