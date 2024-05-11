from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType,ArrayType
from pyspark.sql.functions import explode, explode_outer, col, count, size

def main():
    # create a SparkSession
    spark = SparkSession.builder.appName("ReadAllJSONFiles").getOrCreate()

    # Read the JSON file into a DataFrame
    df = spark.read.option("multiline","true").json("input/daily_files/crm_campaign_20230101001500.json")

    df_flat = flatten_json(df)
    campaign_report = df_flat.select(col('id').alias('campaign_id'),
                             col('details_name').alias('campaign_name'),
                             size(col("steps")).alias('number_of_steps'),
                             col('details_schedule').getItem(0).alias('start_date'),
                             col('details_schedule').getItem(1).alias('end_date'))
    campaign_report.printSchema()
    campaign_report.show()

    # Read the JSON file into a DataFrame
    df = spark.read.option("multiline","true").json("input/daily_files/crm_engagement_20230101001500.json")
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

main()