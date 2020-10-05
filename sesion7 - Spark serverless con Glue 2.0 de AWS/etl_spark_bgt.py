# Libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsglue.job import Job

# getting the Job Name

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Creating Glue and Spark Contexts

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#read data in

db_in = "db-spark-bgt"
tbl_in = "data_in"

dyf_salaries = glueContext.create_dynamic_frame.from_catalog(database=db_in, table_name=tbl_in)

df_salaries = dyf_salaries.toDF()

# Processing

df_employees = df_salaries.select("Id", "EmployeeName", "JobTitle", "TotalPay", "Year", "Agency")

df1 = df_employees.groupBy("JobTitle").agg({'TotalPay': 'mean'}).withColumnRenamed('avg(TotalPay)', 'AverageSalary')

df2 = df_employees.groupBy('JobTitle').count().withColumnRenamed('count', 'QuantityEmployees')

df_jobs = df1.join(df2, 'JobTitle', 'left').orderBy("QuantityEmployees", ascending = False).repartition(1)

# save data out

output_dir = "s3://demo-spark-bgt/data-out-job"

df_jobs.write.parquet(output_dir)


job.commit()