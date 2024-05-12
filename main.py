import sys
import os
import boto3
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType,ArrayType
from pyspark.sql.functions import explode, explode_outer, col, count, size
from copyMergeInto import copy_merge_into


config = configparser.ConfigParser()
config.read('config.cfg')

def create_spark_session(profile):
    """Create a Spark session to process the data

    Arguments: 
        profile: AWS/Local profile to use

    Returns:
        spark: a Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    # pass on AWS credentials in sarkcontext (not needed if file is in local environment
    if profile != 'local':
      session = boto3.session.Session(profile_name = profile)
      spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', session.get_credentials().access_key)
      spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey', session.get_credentials().secret_key)
       
    return spark

def read_json_file(spark, filename):
    """Read json file and return as spark dataframe

    Arguments: 
        spark: spark session
        filename: name of file(s)

    Returns:
        df: dataframe with file contents
    """
    df = spark.read.option("multiline","true").json(filename)
    return df

def process_campaign_data(spark, input_data, output_data, vendor):
    """Read json file and return as spark dataframe

    Arguments: 
        spark: spark session
        input_data: input data folder
        vendor: name of the vendor

    Returns:
        none
    """

    filename = input_data + '/' + vendor + '_' + 'campaign' + '_*.json'
    df = spark.read.option('multiline','true').json(filename)
    df.show()

    df_flat = flatten_json(df)
    df_flat.show()

    campaign_report = df_flat.select(col('id').alias('campaign_id'),
                             col('details_name').alias('campaign_name'),
                             size(col("steps")).alias('number_of_steps'),
                             col('details_schedule').getItem(0).alias('start_date'),
                             col('details_schedule').getItem(1).alias('end_date'))
    campaign_report.printSchema()
    campaign_report.show()

    #write report to csv
    temp_dir = output_data + '/temp'
    output_file = output_data + '/campaign_overview.csv'
    campaign_report.write.mode('overwrite').option('header',True).csv(temp_dir)
    copy_merge_into(spark, temp_dir, output_file)


def main(config_name):
    # create a SparkSession
    #spark = SparkSession.builder.appName("ReadAllJSONFiles").getOrCreate()
    # Replace Key with your AWS account accesskey (You can find this on IAM 
    profile = config[config_name]["PROFILE"]
    internal_data = config[config_name]["INTERNAL_DATA"]
    input_data = config[config_name]["INPUT_DATA"]
    output_data = config[config_name]["OUTPUT_DATA"]
    vendor = config[config_name]["VENDOR"]

    # create spark session
    spark = create_spark_session(profile)

    # process campaign list data
    process_campaign_data(spark, input_data, output_data, vendor)

    # Read the JSON file into a DataFrame
    #df = spark.read.option("multiline","true").json(input_data + "/crm_campaign_20230101001500.json")

    #df_flat = flatten_json(df)
    #campaign_report = df_flat.select(col('id').alias('campaign_id'),
    #                         col('details_name').alias('campaign_name'),
    #                         size(col("steps")).alias('number_of_steps'),
    #                         col('details_schedule').getItem(0).alias('start_date'),
    #                         col('details_schedule').getItem(1).alias('end_date'))
    #campaign_report.printSchema()
    #campaign_report.show()

    # Read the JSON file into a DataFrame
    df = spark.read.option("multiline","true").json(input_data + "/crm_engagement_20230101001500.json")
    df.printSchema()
    df.show()


def flatten_json(df):
   """
    Flattens a DataFrame with complex nested fields (Arrays and Structs) by converting them into individual columns.
   
    Parameters:
    - df: The input DataFrame with complex nested fields
   
    Returns:
    - The flattened DataFrame with all complex fields expanded into separate columns.
   """
   # compute Complex Fields (Lists and Structs) in Schema   
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   print(df.schema)
   print("")
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      # if StructType then convert all sub element to columns.
      # i.e. flatten structs
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      # if ArrayType then add the Array Elements as Rows using the explode function
      # i.e. explode Arrays
      #elif (type(complex_fields[col_name]) == ArrayType):    
      #   df=df.withColumn(col_name,explode_outer(col_name))
    
      # recompute remaining Complex Fields in Schema       
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == StructType])
#                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

main('DEFAULT')