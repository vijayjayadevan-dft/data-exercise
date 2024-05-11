# Task
Our CRM vendor is releasing a new API. We have been given a preview of the data. The connection with the API will scrape it daily so we can process it in batches. We have 10 million regular users and expect to reach 35 million individuals across the year.

We would like you to build a PySpark job that will process the API response data and produce two reports. Present your solution as you would in a production system.

# SLA with vendor
- All events are in order
- At-least-once delivery guarantees for all messages
- There is a maximum limit of 256 campaigns
- Campaigns can have steps added but not removed as long as it is live

# ITV Architecture Standards
● Each data product has its own S3 bucket
● API responses are to be sent to an S3 bucket that contains the following folder structure:
○ /input/daily_files - Where the supplier’s files will land in the bucket. The filename
will look like supplier_file_YYYYMMDDHHmmSS.extension (e.g.crm_campaign_20230101001500.json)
● /internal - Can be used to store any internal state that is needed and is not exposed to the end user
● Reports are to be stored in: /output/reports

# Reports Requested
## 1) Current Campaign Engagement Report
| campaign_name        | average_percent_completion | rank |
| -------------------- | -------------------------- | -----|
| summer_romance_binge | 0.40                       | 1    |
| win_back             | 0                          | 2    |


## 2) Campaign Overview
| campaign_id         | campaign_name        | number_of_steps | start_date |end_date |
| ---------------------------------| --------------------|----|-----|-----|
| fd0edd759373110538f003782c49f0d4 | summer_romance_binge| 4  |     |     |
| 548ecdb98fa7f2c024aac57c686936c1 | win_back            | 3  |     |     |

# Preview of Vendor’s new API
[example response from the campaigns/list API](input/daily_files/crm_campaign_20230101001500.json)

[example response from the user/engagement API](input/daily_files/crm_engagement_20230101001500.json)


