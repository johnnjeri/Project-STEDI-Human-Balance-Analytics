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

# Script generated for node accelerometer data
accelerometerdata_node1678589933073 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udsparkproject/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerdata_node1678589933073",
)

# Script generated for node step trainer
steptrainer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udsparkproject/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="steptrainer_node1",
)

# Script generated for node Drop accel Fields
DropaccelFields_node1678590152221 = DropFields.apply(
    frame=accelerometerdata_node1678589933073,
    paths=["user"],
    transformation_ctx="DropaccelFields_node1678590152221",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678590364580 = DynamicFrame.fromDF(
    steptrainer_node1.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678590364580",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678590323949 = DynamicFrame.fromDF(
    DropaccelFields_node1678590152221.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678590323949",
)

# Script generated for node Change Schema
ChangeSchema_node1678817031841 = ApplyMapping.apply(
    frame=DropDuplicates_node1678590364580,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "timestamp"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="ChangeSchema_node1678817031841",
)

# Script generated for node Change Schema
ChangeSchema_node1678817011247 = ApplyMapping.apply(
    frame=DropDuplicates_node1678590323949,
    mappings=[
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
        ("timeStamp", "string", "timeStamp", "timestamp"),
    ],
    transformation_ctx="ChangeSchema_node1678817011247",
)

# Script generated for node Join
Join_node1678590200375 = Join.apply(
    frame1=ChangeSchema_node1678817011247,
    frame2=ChangeSchema_node1678817031841,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1678590200375",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678590417289 = DynamicFrame.fromDF(
    Join_node1678590200375.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678590417289",
)

# Script generated for node save ml file
savemlfile_node1678590513140 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678590417289,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udsparkproject/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="savemlfile_node1678590513140",
)

job.commit()
