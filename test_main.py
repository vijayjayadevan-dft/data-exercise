import pytest
from main import create_spark_session, get_campaign_list_data, get_campaign_engagement_data


@pytest.mark.parametrize("profile", ["local", "dev", "prod"])
def test_create_spark_session(profile):
    """Test if the function creates a Spark session successfully."""
    spark = create_spark_session(profile)
    assert spark is not None
    assert spark.sparkContext.appName == "process_crm_data"


def test_get_campaign_list_data(spark_session, input_data, mocker):
    """Test if the function reads and processes campaign list data correctly."""
    mocker.patch("main.spark.read.option")
    mocker.patch("main.spark.read.json")
    df = spark_session.createDataFrame([("id1", "campaign1", 3, "2023-01-01", "2023-01-31")],
                                       ["campaign_id", "campaign_name", "number_of_steps", "start_date", "end_date"])
    mocker.patch("main.spark.read.json.return_value", df)
    result_df = get_campaign_list_data(spark_session)
    assert result_df.count() == 1
    assert result_df.collect()[0]["campaign_name"] == "campaign1"