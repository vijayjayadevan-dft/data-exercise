import pytest
from unittest.mock import patch
from main import create_spark_session, flatten_json, write_output_report, get_campaign_engagement_data, get_campaign_list_data, process_api_data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType, DoubleType, LongType
from pyspark.sql import SparkSession,SQLContext
from pyspark.testing import assertDataFrameEqual
import gzip
import os


@pytest.fixture
def mock_boto3_session():
    with patch("boto3.session.Session") as mock_session:
        mock_session.return_value.get_credentials.return_value.access_key = "mock_access_key"
        mock_session.return_value.get_credentials.return_value.secret_key = "mock_secret_key"
        yield mock_session

def test_create_spark_session_local(mock_boto3_session):
    """Test create_spark_session function with local profile"""
    spark = create_spark_session("local")
    assert spark.sparkContext.appName == "process_crm_data"
    assert spark.sparkContext.getConf().get("spark.jars.packages") == "org.apache.hadoop:hadoop-aws:2.7.0"
    mock_boto3_session.assert_not_called()

@pytest.mark.parametrize("profile", ["dev", "prod"])
def test_create_spark_session_non_local(profile, mock_boto3_session):
    """Test create_spark_session function with non-local profiles"""
    spark = create_spark_session(profile)
    assert spark.sparkContext.appName == "process_crm_data"
    assert spark.sparkContext.getConf().get("spark.jars.packages") == "org.apache.hadoop:hadoop-aws:2.7.0"
    mock_boto3_session.assert_called_once_with(profile_name=profile)
    assert spark.sparkContext._jsc.hadoopConfiguration().get('fs.s3n.awsAccessKeyId') == "mock_access_key"
    assert spark.sparkContext._jsc.hadoopConfiguration().get('fs.s3n.awsSecretAccessKey') == "mock_secret_key"

def test_create_spark_session_invalid_profile():
    """Test create_spark_session function with invalid profile"""
    with pytest.raises(Exception):
        create_spark_session("invalid_profile")

@pytest.fixture(scope="session")
def spark_session():
    """
    Creates a Spark session with local profile for testing purposes.
    """
    spark = create_spark_session("local")    
    yield spark

    spark.stop()

@pytest.fixture
def mock_df(spark_session):
    data = [
        {"id": 1, "details": {"name": "Campaign A", "schedule": ["2023-01-01", "2023-01-31"]}},
        {"id": 2, "details": {"name": "Campaign B", "schedule": ["2023-02-01", "2023-02-28"]}},
    ]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("details", StructType([
            StructField("name", StringType(), True),
            StructField("schedule", ArrayType(StringType()), True),
        ]), True),
    ])
    return spark_session.createDataFrame(data, schema)

def test_flatten_json_struct(mock_df):
    """Test flatten_json function with a struct column"""
    flattened_df = flatten_json(mock_df, "details")
    assert flattened_df.columns == ["id", "details_name", "details_schedule"]
    assert flattened_df.collect()[0]["details_name"] == "Campaign A"
    assert flattened_df.collect()[1]["details_schedule"] == ["2023-02-01", "2023-02-28"]

@pytest.fixture
def mock_df_array(spark_session):
    data = [
        {"id": 1, "steps": ["Step 1", "Step 2", "Step 3"]},
        {"id": 2, "steps": ["Step 4", "Step 5"]},
    ]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("steps", ArrayType(StringType()), True),
    ])
    return spark_session.createDataFrame(data, schema)

def test_flatten_json_array(mock_df_array):
    """Test flatten_json function with an array column"""
    flattened_df = flatten_json(mock_df_array, "steps")
    assert flattened_df.columns == ["id", "steps"]
    assert flattened_df.collect()[0]["steps"] == "Step 1"
    assert flattened_df.collect()[2]["steps"] == "Step 3"
    assert flattened_df.collect()[4]["steps"] == "Step 5"



@pytest.fixture
def mock_output_df(spark_session):
    """Mock output file path for testing."""
    output_data = [
        {"campaign_name": "Campaign A", "average_percent_completion": 0.5},
        {"campaign_name": "Campaign B", "average_percent_completion": 1.0},
    ]
    schema = StructType([
        StructField("campaign_name", StringType(), True),
        StructField("average_percent_completion", FloatType(), True),
    ])
    output_df = spark_session.createDataFrame(output_data, schema)
    output_df = output_df.orderBy("campaign_name")
    return output_df

@pytest.fixture
def mock_output_file():
    """Mock output file path for testing."""
    return "test_data/mock_output_file.csv.gzip"

def test_write_output_report(spark_session, mock_output_df, mock_output_file):
    """Test the `write_output_report` function with mock data and content validation."""
    write_output_report(spark_session, mock_output_df, mock_output_file)

    # Verify that the output file was created and contains the expected content.
    with gzip.open(mock_output_file, 'rb') as infile:
        lines = infile.readlines()
        assert lines[0] == b'campaign_name,average_percent_completion\n'
        assert lines[1] == b'Campaign A,0.5\n'
        assert lines[2] == b'Campaign B,1.0\n'

    # Clean up the temporary file.
    os.remove(mock_output_file)


@pytest.fixture
def mock_expected_campaign_engagement_df(spark_session):
    """Mock expected campaign engagement data for testing."""
    expected_campaign_engagement = [
    {"campaign": "campaign1", "num_of_users": 2, "num_of_active_users": 1, "average_percent_completion": 0.5},
    {"campaign": "campaign2", "num_of_users": 2, "num_of_active_users": 2, "average_percent_completion": 1.0},
    ]
    schema = StructType([
        StructField("campaign", StringType(), True),
        StructField("num_of_users", LongType(), True),
        StructField("num_of_active_users", LongType(), True),
        StructField("average_percent_completion", DoubleType(), True),
      ])
    output_df = spark_session.createDataFrame(expected_campaign_engagement, schema)
    output_df = output_df.orderBy("campaign")
    return output_df

@pytest.fixture
def mock_engagement_list_file():
    """Mock engagement list file path for testing."""
    return 'test_data/mock_engagement_list.json'

def test_get_campaign_engagement_data(spark_session, mock_engagement_list_file, mock_expected_campaign_engagement_df):
    """Test get_campaign_engagement_data function with mock data."""
    # Call the function with mocked data
    result_df = get_campaign_engagement_data(spark_session, mock_engagement_list_file)
    result_df = result_df.orderBy("campaign")
    # Assert that the result matches the expected output
    assertDataFrameEqual(result_df, mock_expected_campaign_engagement_df)
    #assert result_df == mock_expected_campaign_engagement_df



@pytest.fixture
def mock_expected_campaign_list_df(spark_session):
    """Mock expected campaign list data for testing."""
    expected_campaign_list = [
        {"campaign_id": "campaign1", "campaign_name": "Campaign A", "number_of_steps": 2, "start_date": "2023-10-10", "end_date": "2023-10-31"},
        {"campaign_id": "campaign2", "campaign_name": "Campaign B", "number_of_steps": 4, "start_date": "2023-08-15", "end_date": "2023-09-25"},
    ]
    schema = StructType([
        StructField("campaign_id", StringType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("number_of_steps", IntegerType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
      ])
    output_df = spark_session.createDataFrame(expected_campaign_list, schema)
    output_df = output_df.orderBy("campaign_id")
    return output_df

@pytest.fixture
def mock_campaign_list_file():
    """Mock campaign list file path for testing."""
    return 'test_data/mock_campaign_list.json'

# Test get_campaign_list_data with mock data
def test_get_campaign_list_data(spark_session, mock_campaign_list_file, mock_expected_campaign_list_df):
    """Test test_get_campaign_list_data function with mock data."""
    # Call the function with the mock DataFrame
    result_df = get_campaign_list_data(spark_session, mock_campaign_list_file)
    result_df = result_df.orderBy("campaign_id")
    # Assert that the result matches the expected output
    assertDataFrameEqual(result_df, mock_expected_campaign_list_df)


@pytest.fixture
def mock_output_campaign_overview_report():
    """Mock output campaign overview report path for testing."""
    return "data/output/reports/campaign_overview_report.csv.gzip"

@pytest.fixture
def mock_output_campaign_engagement_report():
    """Mock output campaign engagement report path for testing."""
    return "data/output/reports/current_campaign_engagement_report.csv.gzip"

# Test process_api_data with preview data
def test_process_api_data(mock_output_campaign_overview_report, mock_output_campaign_engagement_report):
    """Test process_api_data function."""

    # Clean up the existing output files
    if os.path.exists(mock_output_campaign_overview_report):
        os.remove(mock_output_campaign_overview_report)
    if os.path.exists(mock_output_campaign_engagement_report):
        os.remove(mock_output_campaign_engagement_report)

    # Call the function with the mock DataFrame
    process_api_data()

    # Verify that the campaign_overview report contains the expected content.
    with gzip.open(mock_output_campaign_overview_report, 'rb') as infile:
        lines = infile.readlines()
        assert lines[0] == b'campaign_id,campaign_name,number_of_steps,start_date,end_date\n'
        assert lines[1] == b'6fg7e8,summer_romance_binge,4,2023-07-21,2023-07-31\n'
        assert lines[2] == b'cb571,win_back,3,2023-07-01,2023-07-25\n'

    # Verify that the campaign_engagement report contains the expected content.
    with gzip.open(mock_output_campaign_engagement_report, 'rb') as infile:
        lines = infile.readlines()
        assert lines[0] == b'campaign_name,average_percent_completion,rank\n'
        assert lines[1] == b'summer_romance_binge,0.4,1\n'
        assert lines[2] == b'win_back,0.0,2\n'

