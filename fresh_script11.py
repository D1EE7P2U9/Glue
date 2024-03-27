import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job



input_path = 's3:--input_path--'
output_path = 's3:--output_path--'



args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"quoteChar":"\"","escaper":"","withHeader":True,"optimizePerformance":False,"separator":","}, connection_type = "s3", format = "csv", connection_options = {"paths": [input_path], "recurse":True}, transformation_ctx = "DataSource0")

Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [], transformation_ctx = "Transform0")

DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "json", connection_options = {"path": output_path, "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()



