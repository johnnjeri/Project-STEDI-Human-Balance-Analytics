import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer source
accelerometersource_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udsparkproject/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometersource_node1",
)

# Script generated for node tursted customers source
turstedcustomerssource_node1678409712718 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://udsparkproject/customers/trusted/"],
            "recurse": True,
        },
        transformation_ctx="turstedcustomerssource_node1678409712718",
    )
)

# Script generated for node inner join (trusted only)
innerjointrustedonly_node2 = Join.apply(
    frame1=turstedcustomerssource_node1678409712718,
    frame2=accelerometersource_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="innerjointrustedonly_node2",
)

# Script generated for node Drop Fields
DropFields_node1678747868743 = DropFields.apply(
    frame=innerjointrustedonly_node2,
    paths=["x", "y", "user", "z"],
    transformation_ctx="DropFields_node1678747868743",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678759230728 = DynamicFrame.fromDF(
    DropFields_node1678747868743.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678759230728",
)

# Script generated for node output
output_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678759230728,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udsparkproject/customers/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="output_node3",
)

job.commit()
