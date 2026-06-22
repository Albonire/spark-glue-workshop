# scripts/job_silver_to_gold.py
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
BUCKET = args["BUCKET"]
sc = SparkContext()
global_context = GlueContext(sc)
spark = global_context.spark_session
logger = global_context.get_logger()
job = Job(global_context)
job.init(args["JOB_NAME"], args)

try:
    logger.info("STEP 2: Reading Silver Parquet")
    df = spark.read.parquet(f"s3://{BUCKET}/silver/")
    logger.info(f"STEP 2: Read {df.count()} rows from Silver")
except Exception as e:
    logger.error(f"STEP 2 FAILED: {e}")
    raise

try:
    logger.info("STEP 3: Building dim_product")
    dim_product = (df
        .select("stock_code", "description")
        .dropDuplicates(["stock_code"])
        .withColumn("product_sk", F.monotonically_increasing_id()))
    dim_product.write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/dim_product/")
    logger.info("STEP 3: dim_product written")
except Exception as e:
    logger.error(f"STEP 3 FAILED: {e}")
    raise

try:
    logger.info("STEP 4: Building dim_customer")
    dim_customer = (df
        .select("customer_id", "country")
        .dropDuplicates(["customer_id"])
        .withColumn("customer_sk", F.monotonically_increasing_id()))
    dim_customer.write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/dim_customer/")
    logger.info("STEP 4: dim_customer written")
except Exception as e:
    logger.error(f"STEP 4 FAILED: {e}")
    raise

try:
    logger.info("STEP 5: Building dim_date")
    dim_date = (df
        .select(F.to_date("invoice_date").alias("date"))
        .dropDuplicates()
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("weekday", F.date_format("date", "EEEE"))
        .withColumn("date_sk", F.date_format("date", "yyyyMMdd").cast("int")))
    dim_date.write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/dim_date/")
    logger.info("STEP 5: dim_date written")
except Exception as e:
    logger.error(f"STEP 5 FAILED: {e}")
    raise

try:
    logger.info("STEP 6: Building fact_sales")
    df_with_keys = (df
        .join(F.broadcast(dim_product.select("stock_code", "product_sk")), "stock_code", "left")
        .join(F.broadcast(dim_customer.select("customer_id", "customer_sk")), "customer_id", "left")
        .withColumn("date_sk", F.date_format(F.to_date("invoice_date"), "yyyyMMdd").cast("int"))
        .withColumn("total_amount", F.col("quantity") * F.col("price"))
        .withColumn("is_return", F.col("invoice").startswith("C")))

    fact_sales = df_with_keys.select(
        "invoice", "product_sk", "customer_sk", "date_sk",
        "quantity", F.col("price").alias("unit_price"), "total_amount", "is_return")
    fact_sales.write.mode("overwrite").parquet(f"s3://{BUCKET}/gold/fact_sales/")
    logger.info("STEP 6: fact_sales written")
except Exception as e:
    logger.error(f"STEP 6 FAILED: {e}")
    raise

job.commit()
