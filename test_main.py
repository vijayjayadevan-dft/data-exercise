import unittest
import os
import boto3
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import explode_outer, col, avg, count, count_if, size, rank, desc
from copyMergeInto import copy_merge_into

from main import create_spark_session, get_campaign_list_data, get_campaign_engagement_data, write_output_report, flatten_json, process_api_data

class TestMain(unittest.TestCase):

    def setUp(self):
        self.spark = create_spark_session("local")
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')

    def tearDown(self):
        self.spark.stop()

    def test_create_spark_session(self):
        """Test if the function creates a Spark session successfully."""
        profile = "local"
        spark = create_spark_session(profile)
        self.assertIsNotNone(spark)
        self.assertEqual(spark.sparkContext.appName, "process_crm_data")

    def test_get_campaign_list_data(self):
        """Test if the function reads and processes campaign list data correctly."""
        df = get_campaign_list_data(self.spark)
        self.assertEqual(df.count(), 1)
        self.assertEqual(df.collect()[0]["campaign_name"], "campaign1")

    def test_get_campaign_engagement_data(self):
        """Test if the function reads and processes user engagement data correctly."""
        df = get_campaign_engagement_data(self.spark)
        self.assertEqual(df.count(), 1)
        self.assertEqual(df.collect()[0]["num_of_users"], 2)

    def test_write_output_report(self):
        """Test if the function writes the output report correctly."""
        df = self.spark.createDataFrame([("campaign1", "user1", 10, 2, 5, 17),
                                       ("campaign1", "user2", 5, 1, 3, 9)],
                                       ["campaign", "userid", "messages_delivered", "messages_failed", "messages_opened", "num_of_actions"])
        output_file = "output.csv"
        write_output_report(self.spark, df, output_file)
        self.assertTrue(os.path.isfile(output_file))

    def test_flatten_json(self):
        """Test if the function flattens nested JSON correctly."""
        df = self.spark.createDataFrame([({"name": "John", "age": 30}, [1, 2, 3])], ["details", "steps"])
        result_df = flatten_json(df, "details")
        self.assertEqual(result_df.count(), 1)
        self.assertEqual(result_df.collect()[0]["details_name"], "John")
        self.assertEqual(result_df.collect()[0]["details_age"], 30)
        self.assertEqual(result_df.collect()[0]["steps"], [1, 2, 3])

    def test_process_api_data(self):
        """Test if the function processes API data and outputs reports correctly."""
        process_api_data()
        # Check if output files exist
        output_file1 = self.config['DATA']['output_data'] + '/campaign_overview.csv'
        output_file2 = self.config['DATA']['output_data'] + '/current_campaign_engagement_report.csv'
        self.assertTrue(os.path.isfile(output_file1))
        self.assertTrue(os.path.isfile(output_file2))

if __name__ == '__main__':
    unittest.main()
