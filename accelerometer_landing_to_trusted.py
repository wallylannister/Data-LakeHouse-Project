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

# Script generated for node Customer Trusted
CustomerTrusted_node1732532761173 = glueContext.create_dynamic_frame.from_catalog(database="stedi-lakehouse", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1732532761173")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1732531001900 = glueContext.create_dynamic_frame.from_catalog(database="stedi-lakehouse", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1732531001900")

# Script generated for node Join Tables
SqlQuery976 = '''
select * from AL join CT 
on AL.user = CT.email
'''
JoinTables_node1732531035958 = sparkSqlQuery(glueContext, query = SqlQuery976, mapping = {"AL":AccelerometerLanding_node1732531001900, "CT":CustomerTrusted_node1732532761173}, transformation_ctx = "JoinTables_node1732531035958")

# Script generated for node Drop Duplicates & Unused Fields
SqlQuery977 = '''
select distinct user, timestamp, x, y, z from JT
'''
DropDuplicatesUnusedFields_node1732533437365 = sparkSqlQuery(glueContext, query = SqlQuery977, mapping = {"JT":JoinTables_node1732531035958}, transformation_ctx = "DropDuplicatesUnusedFields_node1732533437365")

# Script generated for node Amazon S3
AmazonS3_node1732534289801 = glueContext.getSink(path="s3://data-lakehouse-hba/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732534289801")
AmazonS3_node1732534289801.setCatalogInfo(catalogDatabase="stedi-lakehouse",catalogTableName="accelerometer_trusted")
AmazonS3_node1732534289801.setFormat("json")
AmazonS3_node1732534289801.writeFrame(DropDuplicatesUnusedFields_node1732533437365)
job.commit()