import pytest
from unittest.mock import patch
from main import create_spark_session, flatten_json, write_output_report
import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType
from pyspark.sql import SparkSession,SQLContext



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
    return output_df

@pytest.fixture
def mock_output_file():
    """Mock output file path for testing."""
    return "test_data/mock_output_file.csv"

def test_write_output_report(spark_session, mock_output_df, mock_output_file):
    """Test the `write_output_report` function with mock data and content validation."""
    write_output_report(spark_session, mock_output_df, mock_output_file)

    # Verify that the output file was created and contains the expected content.
    with open(mock_output_file, 'r') as f:
        lines = f.readlines()
        assert lines[0] == "campaign_name,average_percent_completion\n"
        assert lines[1] == "Campaign A,0.5\n"
        assert lines[2] == "Campaign B,1.0\n"

    # Clean up the temporary file.
    os.remove(mock_output_file)