import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1732560875170 = glueContext.create_dynamic_frame.from_catalog(database="stedi-lakehouse", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1732560875170")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1732560856708 = glueContext.create_dynamic_frame.from_catalog(database="stedi-lakehouse", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1732560856708")

# Script generated for node SQL Query
SqlQuery808 = '''
select * from at join st on
at.timestamp = st.sensorreadingtime
'''
SQLQuery_node1732560935824 = sparkSqlQuery(glueContext, query = SqlQuery808, mapping = {"at":AccelerometerTrusted_node1732560856708, "st":StepTrainerTrusted_node1732560875170}, transformation_ctx = "SQLQuery_node1732560935824")

# Script generated for node Amazon S3
AmazonS3_node1732561125996 = glueContext.getSink(path="s3://data-lakehouse-hba/ml_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732561125996")
AmazonS3_node1732561125996.setCatalogInfo(catalogDatabase="stedi-lakehouse",catalogTableName="machine_learning_curated")
AmazonS3_node1732561125996.setFormat("json")
AmazonS3_node1732561125996.writeFrame(SQLQuery_node1732560935824)
job.commit()