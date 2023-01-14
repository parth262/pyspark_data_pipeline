from dataset.source.file_source import FileSource, FileSourceContext
from dataset.sink.hudi_sink import HudiSink, HudiSinkContext
from dataset.sink.jdbc_sink import JDBCSink, JDBCSinkContext
from ingestion import IngestionPipeline, IngestionContext
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f

source_file_path = "C:/Users/parth/Desktop/workspace/data_pipeline/resources/sample_data/customers_full.csv"
target_file_path = "C:/Users/parth/Desktop/workspace/data_pipeline/output/customers"
table_name = "customers"
partition_from = "created_date"
partition_column = "date"
primary_key_field = "id"
precombine_field = "updated_date"
spark_write_mode = "overwrite"


def prepare_hudi_sink():
    hudi_sink_context = HudiSinkContext(
        spark_write_mode,
        table_name,
        primary_key_field,
        partition_column,
        "upsert",
        precombine_field,
        target_file_path
    )
    return HudiSink(hudi_sink_context)


def prepare_jdbc_sink():
    jdbc_sink_context = JDBCSinkContext(
        "overwrite",
        "com.mysql.cj.jdbc.Driver",
        "jdbc:mysql://localhost:3306/test",
        table_name,
        "root",
        "root"
    )
    return JDBCSink(jdbc_sink_context)


def transform(df: DataFrame) -> DataFrame:
    timestamp_columns = ["created_date", "updated_date"]
    w_create_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for column in timestamp_columns:
        df = df.withColumn(column, f.col(column).cast("timestamp"))

    return (
        df.withColumn(partition_column, f.date_format(f.col(partition_from), "yyyyMMdd"))
        .withColumn("w_create_ts", f.lit(w_create_ts).cast("timestamp"))
    )


spark = (
    SparkSession.Builder()
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.2-bundle_2.12:0.12.2,mysql:mysql-connector-java:8.0.31")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .appName("Local app")
    .master("local")
    .getOrCreate()
)


source_context = FileSourceContext("csv", {"header": "true", "inferSchema": "true"}, source_file_path)
source = FileSource(source_context)

jdbc_sink = prepare_jdbc_sink()

ingestion_context = IngestionContext(
    source,
    transform,
    jdbc_sink
)

ingestion_pipeline = IngestionPipeline(ingestion_context)
ingestion_pipeline.run(spark)
