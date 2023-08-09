import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-jchapto/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1690249397300 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-jchapto/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1690249397300",
)

# Script generated for node Join
Join_node1690249507269 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1690249397300,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1690249507269",
)

# Script generated for node Drop Fields
DropFields_node1690249946062 = DropFields.apply(
    frame=Join_node1690249507269,
    paths=[
        "`.serialNumber`",
        "birthDay",
        "timeStamp",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1690249946062",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1690249946062,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-jchapto/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
