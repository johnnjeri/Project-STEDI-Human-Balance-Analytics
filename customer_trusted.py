import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer data source
customerdatasource_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udsparkproject/customers/landing/"],
        "recurse": True,
    },
    transformation_ctx="customerdatasource_node1",
)

# Script generated for node filter out pii
filteroutpii_node2 = Filter.apply(
    frame=customerdatasource_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="filteroutpii_node2",
)

# Script generated for node destination
destination_node3 = glueContext.write_dynamic_frame.from_options(
    frame=filteroutpii_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udsparkproject/customers/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="destination_node3",
)

job.commit()
