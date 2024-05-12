import pytest
from unittest.mock import patch
from main import create_spark_session, flatten_json, main, get_campaign_engagement_data, write_output_report, get_campaign_list_data, read_json_file

@pytest.mark.parametrize("profile, expected_app_name", [
    ("local", "ReadAllJSONFiles"),
    ("aws", "ReadAllJSONFiles"),
])
def test_create_spark_session(profile, expected_app_name):
    with patch("pyspark.sql.SparkSession") as mock_spark_session:
        create_spark_session(profile)
        mock_spark_session.builder.appName.assert_called_once_with(expected_app_name)
        mock_spark_session.builder.getOrCreate.assert_called_once()

@pytest.mark.parametrize("profile, expected_access_key, expected_secret_key", [
    ("aws", "access_key", "secret_key"),
])
def test_create_spark_session_aws(profile, expected_access_key, expected_secret_key):
    with patch("pyspark.sql.SparkSession") as mock_spark_session, \
         patch("boto3.session.Session") as mock_boto_session:
        create_spark_session(profile)
        mock_spark_session.builder.appName.assert_called_once_with("ReadAllJSONFiles")
        mock_spark_session.builder.getOrCreate.assert_called_once()
        mock_boto_session.assert_called_once_with(profile_name=profile)
        mock_spark_session.sparkContext._jsc.hadoopConfiguration().set.assert_has_calls([
            call('fs.s3n.awsAccessKeyId', expected_access_key),
            call('fs.s3n.awsSecretAccessKey', expected_secret_key)
        ])

@pytest.mark.parametrize("df, col_name, expected_type", [
    (mock_spark_session.return_value.DataFrame, "details", StructType),
    (mock_spark_session.return_value.DataFrame, "steps", ArrayType),
])
def test_flatten_json(df, col_name, expected_type):
    with patch("pyspark.sql.SparkSession") as mock_spark_session:
        flatten_json(df, col_name)
        # ... (assertions on column expansion or array explosion)

class TestMain(unittest.TestCase):

    @patch('configparser.ConfigParser')
    @patch('main.create_spark_session')
    @patch('main.get_campaign_list_data')
    @patch('main.get_campaign_engagement_data')
    @patch('main.write_output_report')
    def test_main(self, mock_write_output_report, mock_get_campaign_engagement_data, mock_get_campaign_list_data, mock_create_spark_session, mock_config_parser):
        config_name = 'DEFAULT'
        main(config_name)
        mock_config_parser.ConfigParser.return_value.read.assert_called_once_with('config.cfg')
        mock_create_spark_session.assert_called_once_with(mock_config_parser.ConfigParser.return_value['DEFAULT']['PROFILE'])
        mock_get_campaign_list_data.assert_called_once_with(mock_create_spark_session.return_value, mock_config_parser.ConfigParser.return_value['DEFAULT']['INPUT_DATA'], mock_config_parser.ConfigParser.return_value['DEFAULT']['VENDOR'])
        mock_get_campaign_engagement_data.assert_called_once_with(mock_create_spark_session.return_value, mock_config_parser.ConfigParser.return_value['DEFAULT']['INPUT_DATA'], mock_config_parser.ConfigParser.return_value['DEFAULT']['VENDOR'])
        # ... (further assertions on report generation)


class TestGetCampaignEngagementData(unittest.TestCase):

    @patch('pyspark.sql.SparkSession')
    def test_get_campaign_engagement_data(self, mock_spark_session):
        spark = mock_spark_session.return_value
        input_data = 'input_data'
        vendor = 'vendor'
        get_campaign_engagement_data(spark, input_data, vendor)
        spark.read.option.assert_called_once_with('multiline', 'true')
        spark.read.json.assert_called_once_with(input_data + '/' + vendor + '_' + 'engagement' + '_*.json')
        # ... (further assertions on data processing)

class TestWriteOutputReport(unittest.TestCase):

    @patch('pyspark.sql.SparkSession')
    def test_write_output_report(self, mock_spark_session):
        spark = mock_spark_session.return_value
        output_df = mock_spark_session.return_value.DataFrame
        output_file = 'output.csv'
        write_output_report(spark, output_df, output_file)
        output_df.write.mode.assert_called_once_with('overwrite')
        output_df.write.option.assert_called_once_with('header', True)
        output_df.write.csv.assert_called_once_with('temp')
        # ... (further assertions on copy_merge_into)

class TestGetCampaignListData(unittest.TestCase):

    @patch('pyspark.sql.SparkSession')
    def test_get_campaign_list_data(self, mock_spark_session):
        spark = mock_spark_session.return_value
        input_data = 'input_data'
        vendor = 'vendor'
        get_campaign_list_data(spark, input_data, vendor)
        spark.read.option.assert_called_once_with('multiline', 'true')
        spark.read.json.assert_called_once_with(input_data + '/' + vendor + '_' + 'campaign' + '_*.json')
        # ... (further assertions on data processing)

class TestReadJsonFile(unittest.TestCase):

    @patch('pyspark.sql.SparkSession')
    def test_read_json_file(self, mock_spark_session):
        spark = mock_spark_session.return_value
        filename = 'test.json'
        read_json_file(spark, filename)
        spark.read.option.assert_called_once_with("multiline", "true")
        spark.read.json.assert_called_once_with(filename)