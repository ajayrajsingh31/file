import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Logging to verify data source
print("Creating DynamicFrame from AWS Glue Data Catalog")
AWSGlueDataCatalog_node1726834910786 = glueContext.create_dynamic_frame.from_catalog(database="product", table_name="s3_ajay_bucket", transformation_ctx="AWSGlueDataCatalog_node1726834910786")

# Logging to verify Drop Fields transformation
print("Dropping unnecessary fields")
DropFields_node1726834923675 = DropFields.apply(frame=AWSGlueDataCatalog_node1726834910786, paths=["displacement", "horsepower", "weight", "acceleration", "modelyear"], transformation_ctx="DropFields_node1726834923675")

# Logging to verify Redshift write operation
print("Writing data to Amazon Redshift")
AmazonRedshift_node1726834934314 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1726834923675, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-654654548180-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.car_data", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS public.car_data (mpg DOUBLE PRECISION, cylinders BIGINT, origin BIGINT, carname VARCHAR); TRUNCATE TABLE public.car_data;"}, transformation_ctx="AmazonRedshift_node1726834934314")

# Read data back from Redshift
print("Reading data back from Amazon Redshift")
redshift_read_options = {
    "url": "jdbc:redshift://redshift-cluster-1.c8bqhiunyq5t.ap-south-1.redshift.amazonaws.com:5439/dev",
    "dbtable": "public.car_data",
    "user": "awsuser",
    "password": "Admin2024",
    "redshiftTmpDir": "s3://aws-glue-assets-654654548180-ap-south-1/temporary/"
}
redshift_dynamic_frame = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options=redshift_read_options, transformation_ctx="redshift_dynamic_frame")

# Apply transformation (select specific columns)
print("Applying transformation to select specific columns")
transformed_dynamic_frame = redshift_dynamic_frame.select_fields(["mpg", "cylinders", "carname"])

# Logging to verify MySQL write operation
print("Writing transformed data to MySQL")
mysql_write_options = {
    "url": "jdbc:mysql://database-1.c5eeyay2kmws.ap-south-1.rds.amazonaws.com:3306/infra",
    "dbtable": "car_data",
    "user": "admin",
    "password": "admin2024",
    "preactions": """
        CREATE TABLE IF NOT EXISTS car_data (
            mpg DOUBLE PRECISION,
            cylinders BIGINT,
            carname VARCHAR(255)
        );
    """
}
MySQL_node1726835098466 = glueContext.write_dynamic_frame.from_options(frame=transformed_dynamic_frame, connection_type="mysql", connection_options=mysql_write_options, transformation_ctx="MySQL_node1726835098466")

job.commit()
