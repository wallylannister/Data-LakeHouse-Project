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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1732500824846 = glueContext.create_dynamic_frame.from_catalog(database="stedi-lakehouse", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1732500824846")

# Script generated for node Privacy Filter
SqlQuery707 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
PrivacyFilter_node1732529151125 = sparkSqlQuery(glueContext, query = SqlQuery707, mapping = {"myDataSource":AWSGlueDataCatalog_node1732500824846}, transformation_ctx = "PrivacyFilter_node1732529151125")

# Script generated for node Amazon S3
AmazonS3_node1732529373679 = glueContext.getSink(path="s3://data-lakehouse-hba/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732529373679")
AmazonS3_node1732529373679.setCatalogInfo(catalogDatabase="stedi-lakehouse",catalogTableName="customer_trusted")
AmazonS3_node1732529373679.setFormat("json")
AmazonS3_node1732529373679.writeFrame(PrivacyFilter_node1732529151125)
job.commit()