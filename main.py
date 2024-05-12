import sys
import os
import boto3
import configparser
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import explode, explode_outer, col, count,countDistinct, size, rank, desc, lit
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

def get_campaign_list_data(spark, input_data, vendor):
    """process campaign list data from API and return campaign overview data

    Arguments: 
        spark: spark session
        input_data: input data folder
        vendor: name of the vendor

    Returns:
        none
    """
    # read campaign list files
    filename = input_data + '/' + vendor + '_' + 'campaign' + '_*.json'
    df = spark.read.option('multiline','true').json(filename)

    # flatten all the struct fields 
    df_flat = flatten_json(df, 'details')
    #df_flat = flatten_json(df, 'steps')
    df_flat.show()

    campaign_list = df_flat.select(col('id').alias('campaign_id'),
                             col('details_name').alias('campaign_name'),
                             size(col("steps")).alias('number_of_steps'),
                             col('details_schedule').getItem(0).alias('start_date'),
                             col('details_schedule').getItem(1).alias('end_date'))
    campaign_list.printSchema()
    campaign_list.show()

    return campaign_list

def write_output_report(spark, output_df, output_file):
    temp_dir = 'temp'
    output_df.write.mode('overwrite').option('header',True).csv(temp_dir)
    copy_merge_into(spark, temp_dir, output_file)

def get_campaign_engagement_data(spark, input_data, vendor):
    """process user engagement data from API and return campaign engagement data

    Arguments: 
        spark: spark session
        input_data: input data folder
        vendor: name of the vendor

    Returns:
        none
    """
    # read campaign list files
    filename = input_data + '/' + vendor + '_' + 'engagement' + '_*.json'
    df = spark.read.option('multiline','true').json(filename)


    user_report = df.groupBy('campaign').agg(countDistinct('userid').alias('num_of_users'), count('action').alias('num_of_actions'))
    #user_report = user_report.withColumnRenamed('campaign', 'campaign_id')
    user_report.printSchema()
    user_report.show()

    return user_report



    #write report to csv
    #temp_dir = output_data + '/temp'
    #output_file = output_data + '/campaign_overview.csv'
    #campaign_report.write.mode('overwrite').option('header',True).csv(temp_dir)
    #copy_merge_into(spark, temp_dir, output_file)


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

    # get campaign list data
    campaign_list_df = get_campaign_list_data(spark, input_data, vendor)

    # process user engagement data
    user_engagement_df = get_campaign_engagement_data(spark, input_data, vendor)

    # report1 - campaign overview report
    output_file = output_data + '/campaign_overview.csv'
    write_output_report(spark, campaign_list_df, output_file)

    #report2 - campaign enegagement report
    output_file = output_data + '/current_campaign_engagement_report.csv'
    campaign_engagement_df = campaign_list_df.join(user_engagement_df, 
                                campaign_list_df.campaign_id == user_engagement_df.campaign, 
                                "left").drop(user_engagement_df.campaign)
    campaign_engagement_df = campaign_engagement_df.na.fill(value=0)
    campaign_engagement_df = campaign_engagement_df.withColumn('average_percent_completion', lit(0))
    # may not be optimal solution - need to check
    campaign_engagement_df = campaign_engagement_df.withColumn('rank', rank().over(Window.orderBy(desc('num_of_users'))))
    engagement_report = campaign_engagement_df.select('campaign_name','average_percent_completion','rank')
    engagement_report.printSchema()
    engagement_report.show()
    write_output_report(spark, engagement_report, output_file)

def flatten_json(df, col_name):
    """
    Flattens a struct type column to individual columns or array type column to individual rows.
   
    Parameters:
    - df: input DataFrame with complex nested field
    - col_name: name of the column to be falttened
   
    Returns:
    - The flattened DataFrame with all complex fields expanded into separate columns.
    """
    # recompute remaining Complex Fields in Schema       
    complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   
    # if StructType then convert all sub element to columns.
    # i.e. flatten structs
    if (type(complex_fields[col_name]) == StructType):
        expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
        df=df.select("*", *expanded).drop(col_name)
    
    # if ArrayType then add the Array Elements as Rows using the explode function
    # i.e. explode Arrays
    elif (type(complex_fields[col_name]) == ArrayType):    
        df=df.withColumn(col_name,explode_outer(col_name))

    return df

main('DEFAULT')