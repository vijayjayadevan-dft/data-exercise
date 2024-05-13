import boto3
import os
import configparser
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import explode_outer, col, avg, count, count_if, size, rank, desc
from copyMergeInto import copy_merge_into
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get global data from config file
config = configparser.ConfigParser()
config.read('config.ini')

profile = config['ENV']['profile']
input_data = config['DATA']['input_data']
output_data = config['DATA']['output_data']
internal_data = config['DATA']['internal_data']
temp_dir = config['DATA']['temp_dir']
vendor = config['INFO']['vendor']
file_time_stamp = config['INFO']['file_time_stamp']


def create_spark_session(env_profile):
    """Create a Spark session to process the data

    Arguments: 
        env_profile: profile to use for getting aws credentials
    Returns:
        spark: a Spark session
    """
    # create spark sesssion
    spark = SparkSession \
        .builder \
        .appName("process_crm_data") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    # pass on AWS credentials in sarkcontext (not needed for local environment)
    if profile != 'local':
      try:
        session = boto3.session.Session(profile_name = profile)
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', session.get_credentials().access_key)
        spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey', session.get_credentials().secret_key)
      except Exception as e:
        logging.error(f"Error getting AWS credentials: {e}")
        raise e
       
    return spark


def get_campaign_list_data(spark):
    """process campaign list data from API and return campaign overview data

    Arguments: 
        spark: spark session

    Returns:
        campaign overview data dataframe
    """
    # read campaign list files
    filename = input_data + '/' + vendor + '_' + 'campaign' + '_' + file_time_stamp +'*.json'
    try:
      campaign_list_df = spark.read.option('multiline','true').json(filename)
    except Exception as e:
      logging.error(f"Error reading campaign list data: {e}")
      raise e

    # flatten all the struct fields 
    try:
      campaign_list_df  = flatten_json(campaign_list_df , 'details')
    except Exception as e:
      logging.error(f"Error flattening campaign list data: {e}")
      raise e

    # transform and add needed columns 
    campaign_data = campaign_list_df.select(col('id').alias('campaign_id'),
                             col('details_name').alias('campaign_name'),
                             size(col("steps")).alias('number_of_steps'),
                             col('details_schedule').getItem(0).alias('start_date'),
                             col('details_schedule').getItem(1).alias('end_date'))
    return campaign_data


def get_campaign_engagement_data(spark):
    """process user engagement data from API and return campaign engagement data

    Arguments: 
        spark: spark session

    Returns:
        campaign engagement data dataframe
    """
    # read campaign list files
    filename = input_data + '/' + vendor + '_' + 'engagement' + '_' + file_time_stamp +'*.json'
    try:
      engagement_list_df = spark.read.option('multiline','true').json(filename)
    except Exception as e:
      logging.error(f"Error reading campaign engagement data: {e}")
      raise e

    # transform and add needed columns 
    try:
      engagement_data = engagement_list_df .groupBy('campaign','userid').agg(count_if(col('action')  == 'MESSAGE_DELIVERED').alias('messages_delivered'),
                                               count_if(col('action')  == 'DELIVERY_FAILED').alias('messages_failed'),
                                               count_if(col('action')  == 'MESSAGE_OPENED').alias('messages_opened'),
                                               count('action').alias('num_of_actions'))
      engagement_data = engagement_data.withColumn('percent_completion', engagement_data['messages_opened']/(engagement_data['messages_delivered']+(engagement_data['messages_failed']/2)))
    except Exception as e:
      logging.error(f"Error processing campaign engagement data: {e}")
      raise e
    
    try:
      engagement_data = engagement_data.groupBy('campaign').agg(count('userid').alias('num_of_users'),
                                                        count_if(col('messages_opened') > 0).alias('num_of_active_users'),
                                                        avg('percent_completion').alias('average_percent_completion'))
    except Exception as e:
      logging.error(f"Error aggregating campaign engagement data: {e}")
      raise e

    return engagement_data


def write_output_report(spark, output_df, output_file):
    """Write df as output report in csv format

    Arguments: 
        spark: spark session
        output_df: df to be outputted
        output_file: output csv filename

    Returns:
        None
    """
    try:
      output_df.write.mode('overwrite').option('header',True).csv(temp_dir)
      copy_merge_into(spark, temp_dir, output_file)
    except Exception as e:
      logging.error(f"Error writing output report: {e}")
      raise e


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


def process_api_data():
    """Process API data and output reports

    Arguments: 

    Returns:
        None
    """
    # create spark session
    try:
      spark = create_spark_session(profile)
    except Exception as e:
      logging.error(f"Error creating Spark session: {e}")
      raise e

    # get campaign list data
    try:
      campaign_list_df = get_campaign_list_data(spark)
    except Exception as e:
      logging.error(f"Error getting campaign list data: {e}")
      raise e

    # process user engagement data
    try:
      user_engagement_df = get_campaign_engagement_data(spark)
    except Exception as e:
      logging.error(f"Error getting campaign engagement data: {e}")
      raise e

    # report1 - campaign overview report
    output_file = output_data + '/campaign_overview.csv'
    try:
      write_output_report(spark, campaign_list_df, output_file)
    except Exception as e:
      logging.error(f"Error writing campaign overview report: {e}")
      raise e

    #report2 - campaign enegagement report
    try:
      campaign_engagement_df = campaign_list_df.join(user_engagement_df, 
                                      campaign_list_df.campaign_id == user_engagement_df.campaign, 
                                      "left").drop(user_engagement_df.campaign)
      campaign_engagement_df = campaign_engagement_df.na.fill(value=0)
      # window function may not be optimal solution - scope for optimisation?
      campaign_engagement_df = campaign_engagement_df.withColumn('rank', rank().over(Window.orderBy(desc('num_of_active_users'))))
      engagement_report = campaign_engagement_df.select('campaign_name','average_percent_completion','rank')
    except Exception as e:
      logging.error(f"Error processing campaign engagement data: {e}")
      raise e

    output_file = output_data + '/current_campaign_engagement_report.csv'
    try:
      write_output_report(spark, engagement_report, output_file)
    except Exception as e:
      logging.error(f"Error writing campaign engagement report: {e}")
      raise e

process_api_data()