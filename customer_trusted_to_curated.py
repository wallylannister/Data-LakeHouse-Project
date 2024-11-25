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

# Script generated for node Drop Duplicates & PII Fields
SqlQuery1099 = '''
select distinct birthday, 
serialnumber,
registrationdate,
lastupdatedate,
sharewithresearchasofdate,
sharewithpublicasofdate,
sharewithfriendsasofdate from CT
'''
DropDuplicatesPIIFields_node1732533437365 = sparkSqlQuery(glueContext, query = SqlQuery1099, mapping = {"CT":CustomerTrusted_node1732532761173}, transformation_ctx = "DropDuplicatesPIIFields_node1732533437365")

# Script generated for node Amazon S3
AmazonS3_node1732534289801 = glueContext.getSink(path="s3://data-lakehouse-hba/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732534289801")
AmazonS3_node1732534289801.setCatalogInfo(catalogDatabase="stedi-lakehouse",catalogTableName="customer_curated")
AmazonS3_node1732534289801.setFormat("json")
AmazonS3_node1732534289801.writeFrame(DropDuplicatesPIIFields_node1732533437365)
job.commit()