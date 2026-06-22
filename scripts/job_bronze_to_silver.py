                    # scripts/job_bronze_to_silver.py
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]
sc = SparkContext()
global_context = GlueContext(sc)
spark = global_context.spark_session
logger = global_context.get_logger()
job = Job(global_context)
job.init(args["JOB_NAME"], args)

try:
    logger.info("STEP 2: Reading CSVs from Bronze layer")
    df = spark.read.option("header", True).option("inferSchema", False).csv(f"s3://{BUCKET}/bronze/")
    logger.info(f"STEP 2: Read {df.count()} raw rows")
except Exception as e:
    logger.error(f"STEP 2 FAILED: {e}")
    raise

try:
    logger.info("STEP 3: Dropping nulls")
    df = df.dropna(subset=["Invoice", "StockCode"])
except Exception as e:
    logger.error(f"STEP 3 FAILED: {e}")
    raise

try:
    logger.info("STEP 4: Casting columns")
    df = (df
        .withColumn("Quantity", F.col("Quantity").cast("int"))
        .withColumn("Price", F.col("Price").cast("double"))
        .withColumn("Customer ID", F.col("Customer ID").cast("long"))
        .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
        .withColumnRenamed("Customer ID", "customer_id")
        .withColumnRenamed("Invoice", "invoice")
        .withColumnRenamed("StockCode", "stock_code")
        .withColumnRenamed("Description", "description")
        .withColumnRenamed("Quantity", "quantity")
        .withColumnRenamed("InvoiceDate", "invoice_date")
        .withColumnRenamed("Price", "price")
        .withColumnRenamed("Country", "country"))
except Exception as e:
    logger.error(f"STEP 4 FAILED: {e}")
    raise

try:
    logger.info("STEP 5: Filtering invalid rows")
    df = df.filter((F.col("quantity").isNotNull()) & (F.col("price") > 0))
except Exception as e:
    logger.error(f"STEP 5 FAILED: {e}")
    raise

try:
    logger.info("STEP 6: Adding year column")
    df = df.withColumn("year", F.year("invoice_date"))
except Exception as e:
    logger.error(f"STEP 6 FAILED: {e}")
    raise

try:
    logger.info("STEP 7: Writing Silver Parquet")
    df.write.mode("overwrite").partitionBy("year").parquet(f"s3://{BUCKET}/silver/")
    logger.info("STEP 7: Done")
except Exception as e:
    logger.error(f"STEP 7 FAILED: {e}")
    raise

job.commit()
