import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as f
import pyspark.sql.types as types
from datetime import datetime
from awsglue.job import Job
import awswrangler as wr
import pandas as pd


## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

db_username = "admin"
db_password = "xxxxxxxx"

def GetTableDate():
    #get total row count using aws data wrangler
    getTotalRowCount = "SELECT count(*) as totalRowCount from orders"
    df_count = wr.athena.read_sql_query(sql=getTotalRowCount, database="myredshift")
    totalRowCount = df_count.iloc[0,0]
    print("Total row count from Glue Table: ", totalRowCount)
    #get max Order Date using aws data wrangler
    maxOrderDateSql = "SELECT max(o_orderdate) as maxOrderDate FROM orders"
    df = wr.athena.read_sql_query(sql=maxOrderDateSql, database="myredshift")
    maxOrderDate = df.iloc[0,0]
    print("MaxOrderdate from Glue Table: ", maxOrderDate)
    return maxOrderDate

print("Get the max order date from glue table myredshift.orders to create redsfhit query")
maxOrderDate = GetTableDate()
query = "SELECT * from admin.orders where o_orderdate > '{}'".format(maxOrderDate)
print("Query to be executed in redshift: ", query)

#define the redshift connection options
connection_options = {  
    "url": "jdbc:redshift://dwtest.gdhuehkjillo.us-east-1.redshift.amazonaws.com:5439/dwtest",
    "query": query,
    "user": db_username,
    "password": db_password,
    "redshiftTmpDir": args["TempDir"],
    "aws_iam_role": "arn:aws:iam::xxxxxxxxxxxx:role/genomedwtest-s3-access"
}


#glue dynamicframe
df = glueContext.create_dynamic_frame_from_options(connection_type="redshift", connection_options=connection_options)

#get row count
print("Record count from redshift: ", df.toDF().count())

#write the data to S3 location using glue catalog
sink = glueContext.write_dynamic_frame_from_catalog(
                frame=df,
                database="myredshift",
                table_name="orders")

print("Completed writing data to Glue table myredshift.orders")
print("Get the total row count and current max order from Glue table")
GetTableDate()

job.commit()

