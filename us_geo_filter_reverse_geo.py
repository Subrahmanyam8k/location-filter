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
import time

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
        if [ x for x in response_dictionary if response_dictionary[x] == 'US']:
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
all_s3_keys_list = s3.retrieve_objects_list(s3_client, source_s3_bucket , delimiter="/")

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
            #s3_directory=s3_directory[s3_directory.find("/")+1:]
            s3_directory_date=datetime.datetime.strptime(s3_directory, "%Y-%m-%d").strftime("%Y-%m-%d")
            if (archive_start_date<=s3_directory_date) and (s3_directory_date<=archive_end_date):
                filtered_keys.append(s3_key['Prefix'])

if not filtered_keys:
    print("#############################################################################################################################  DATA NOT AVAILABLE IN GIVEN INTERVAL  ###########################################################################################################################################")    

print("Filtered Keys are ###################################################: ",filtered_keys)


for key_path in filtered_keys:
    total_records=0
    unique_records=0
    file_list = s3.retrieve_objects_list(s3_client,source_s3_bucket,prefix=key_path)
    for obj in file_list:
        if not str(obj['Key']).endswith("/"):
            print(obj['Key'])
            whisper_geo_dataset_path = "s3://"+source_s3_bucket+"/"+str(obj['Key'])
        	# Loading csv data
            whisper_geo_df = spark.read.format("csv") \
            .option("inferSchema", "true") \
            .option("sep", "|") \
            .load(whisper_geo_dataset_path)
       	        
    	    us_filter_df=whisper_geo_df.filter(mexico_filter('_c1','_c2'))
    	    total_records=total_records+us_filter_df.count()
    	    unique_records=unique_records+us_filter_df.select("_c13").distinct().count()

    	    #output_directory=key_path[key_path.find("/")+1:]
    	    #output_path="s3a://"+target_s3_bucket+"/"+ output_directory
    
    result=" The Total Records are :::::: " + str(total_records)
    unique_records="    The total unique recorsd are ::: "+str(unique_records)
    final_aggregation=result+unique_records
    filename=time.strftime("%H:%M:%S")
    s3 = boto3.resource('s3')
    object = s3.Object(target_s3_bucket, key_path+"/"+filename+".txt")
    object.put(Body=final_aggregation)
	    
end_time = datetime.datetime.now()
time_difference = end_time - start_time
print(" THE TOTAL EXECUTION TIME IS :::::::",time_difference.seconds)

#logger.info("########################################################################################################################  EXIT THE PIPELINE  #######################################################################################################################################################################")
#except Exception as e:
#logger.exception('Exception occurs while filtering mexico geo data')