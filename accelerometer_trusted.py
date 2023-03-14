import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


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
        "paths": ["s3://udsparkproject/accelerometer/landing/"],
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
    frame1=accelerometersource_node1,
    frame2=turstedcustomerssource_node1678409712718,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="innerjointrustedonly_node2",
)

# Script generated for node Change Schema
ChangeSchema_node1678750125970 = ApplyMapping.apply(
    frame=innerjointrustedonly_node2,
    mappings=[
        ("user", "string", "user", "string"),
        ("timeStamp", "bigint", "timeStamp", "timestamp"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
        ("serialNumber", "string", "serialNumber", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "bigint"),
        ("birthDay", "string", "birthDay", "string"),
        ("registrationDate", "bigint", "registrationDate", "bigint"),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "shareWithResearchAsOfDate",
            "timestamp",
        ),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "bigint"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "bigint"),
    ],
    transformation_ctx="ChangeSchema_node1678750125970",
)

# Script generated for node SQL Query
SqlQuery473 = """
select * from myDataSource
where timeStamp_1 <= shareWithResearchAsOfDate;
"""
SQLQuery_node1678755536033 = sparkSqlQuery(
    glueContext,
    query=SqlQuery473,
    mapping={"myDataSource": ChangeSchema_node1678750125970},
    transformation_ctx="SQLQuery_node1678755536033",
)

# Script generated for node drop fields from customer data
dropfieldsfromcustomerdata_node1678410033480 = DropFields.apply(
    frame=SQLQuery_node1678755536033,
    paths=[
        "shareWithFriendsAsOfDate",
        "phone",
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
    ],
    transformation_ctx="dropfieldsfromcustomerdata_node1678410033480",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1678503769687 = DynamicFrame.fromDF(
    dropfieldsfromcustomerdata_node1678410033480.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1678503769687",
)

# Script generated for node output
output_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1678503769687,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udsparkproject/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="output_node3",
)

job.commit()
