import sys
import os
import boto3
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType,ArrayType
from pyspark.sql.functions import explode, explode_outer, col, count, size


config = configparser.ConfigParser()
config.read('config.cfg')

def create_spark_session(profile):
    """Create a Spark session to process the data

    Arguments: None

    Returns:
        spark: a Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    if profile != 'local':
      session = boto3.session.Session(profile_name = profile)
      spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', session.get_credentials().access_key)
      spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey', session.get_credentials().secret_key)
       
    return spark



def main(config_name):
    # create a SparkSession
    #spark = SparkSession.builder.appName("ReadAllJSONFiles").getOrCreate()
    # Replace Key with your AWS account accesskey (You can find this on IAM 
    profile = config[config_name]["PROFILE"]
    internal_data = config[config_name]["INTERNAL_DATA"]
    input_data = config[config_name]["INPUT_DATA"]
    output_data = config[config_name]["OUTPUT_DATA"]
    spark = create_spark_session(profile)

    # Read the JSON file into a DataFrame
    df = spark.read.option("multiline","true").json(input_data + "/crm_campaign_20230101001500.json")

    df_flat = flatten_json(df)
    campaign_report = df_flat.select(col('id').alias('campaign_id'),
                             col('details_name').alias('campaign_name'),
                             size(col("steps")).alias('number_of_steps'),
                             col('details_schedule').getItem(0).alias('start_date'),
                             col('details_schedule').getItem(1).alias('end_date'))
    campaign_report.printSchema()
    campaign_report.show()

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