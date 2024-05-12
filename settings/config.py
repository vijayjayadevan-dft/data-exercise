class Common(object):
    DEBUG = True

class Local(Common):
    PROFILE='local'
    INPUT_DATA='data/input/daily_files'
    INTERNAL_DATA='data/internal'
    TEMP_DATA='data/internal/temp'
    OUTPUT_DATA='data/output/reports'
    VENDOR='crm'

class Production(Common):
    PROFILE='aws'
    INPUT_DATA='s3://crm_data_product/input/daily_files'
    INTERNAL_DATA='s3://crm_data_product/internal'
    TEMP_DATA='s3://crm_data_product/temp'
    OUTPUT_DATA='s3://crm_data_product/output/reports'
    VENDOR='crm'
    DEBUG = False

class Staging(Production):
    DEBUG = True