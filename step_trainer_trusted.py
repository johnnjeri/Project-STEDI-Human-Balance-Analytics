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

# Script generated for node step trainer trusted
steptrainertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udsparkproject/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainertrusted_node1",
)

# Script generated for node curated customers
curatedcustomers_node1678585746783 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udsparkproject/customers/curated/"],
        "recurse": True,
    },
    transformation_ctx="curatedcustomers_node1678585746783",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1678751338659 = ApplyMapping.apply(
    frame=steptrainertrusted_node1,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "bigint"),
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("distanceFromObject", "int", "`(right) distanceFromObject`", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1678751338659",
)

# Script generated for node Change Schema
ChangeSchema_node1678762573325 = ApplyMapping.apply(
    frame=curatedcustomers_node1678585746783,
    mappings=[
        ("serialNumber", "string", "serialNumber", "string"),
        ("timeStamp", "string", "timeStamp", "bigint"),
        ("birthDay", "string", "birthDay", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "bigint"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="ChangeSchema_node1678762573325",
)

# Script generated for node Join
Join_node1678585847576 = Join.apply(
    frame1=RenamedkeysforJoin_node1678751338659,
    frame2=ChangeSchema_node1678762573325,
    keys1=["sensorReadingTime"],
    keys2=["lastUpdateDate"],
    transformation_ctx="Join_node1678585847576",
)

# Script generated for node Select Fields
SelectFields_node1678805402421 = SelectFields.apply(
    frame=Join_node1678585847576,
    paths=[
        "sensorReadingTime",
        "`(right) serialNumber`",
        "`(right) distanceFromObject`",
    ],
    transformation_ctx="SelectFields_node1678805402421",
)

# Script generated for node Change Schema
ChangeSchema_node1678805463523 = ApplyMapping.apply(
    frame=SelectFields_node1678805402421,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "bigint"),
        ("`(right) serialNumber`", "string", "serialNumber", "string"),
        ("`(right) distanceFromObject`", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="ChangeSchema_node1678805463523",
)

# Script generated for node Save
Save_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1678805463523,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udsparkproject/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Save_node3",
)

job.commit()
