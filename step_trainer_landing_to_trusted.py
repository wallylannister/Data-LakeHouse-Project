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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1732531001900 = glueContext.create_dynamic_frame.from_catalog(database="stedi-lakehouse", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1732531001900")

# Script generated for node Customer Curated
CustomerCurated_node1732552494593 = glueContext.create_dynamic_frame.from_catalog(database="stedi-lakehouse", table_name="customer_curated", transformation_ctx="CustomerCurated_node1732552494593")

# Script generated for node Join Tables
SqlQuery1130 = '''
select sensorreadingtime,
st.serialnumber as step_serial,
distancefromobject from st join cc
on st.serialnumber = cc.serialnumber
'''
JoinTables_node1732531035958 = sparkSqlQuery(glueContext, query = SqlQuery1130, mapping = {"st":StepTrainerLanding_node1732531001900, "cc":CustomerCurated_node1732552494593}, transformation_ctx = "JoinTables_node1732531035958")

# Script generated for node Drop Duplicates
SqlQuery1131 = '''
select distinct sensorreadingtime,
step_serial,
distancefromobject 
from JT
'''
DropDuplicates_node1732533437365 = sparkSqlQuery(glueContext, query = SqlQuery1131, mapping = {"JT":JoinTables_node1732531035958}, transformation_ctx = "DropDuplicates_node1732533437365")

# Script generated for node Amazon S3
AmazonS3_node1732534289801 = glueContext.getSink(path="s3://data-lakehouse-hba/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732534289801")
AmazonS3_node1732534289801.setCatalogInfo(catalogDatabase="stedi-lakehouse",catalogTableName="step_trainer_trusted")
AmazonS3_node1732534289801.setFormat("json")
AmazonS3_node1732534289801.writeFrame(DropDuplicates_node1732533437365)
job.commit()