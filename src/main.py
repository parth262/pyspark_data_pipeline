from dataset.source.file_source import *
from dataset.sink.hudi_sink import *
from ingestion import *

source_file_path = "C:/Users/parth/Desktop/workspace/data_pipeline/resources/sample_data/customers.csv"
target_file_path = "C:/Users/parth/Desktop/workspace/data_pipeline/output/customers"
table_name = "customers"
partition_from = "created_date"
parition_column = "date"
primary_key_field = "id"
precombine_field = "updated_date"
spark_write_mode = "append"

source_context = FileSourceContext("csv", {"header": "true", "inferSchema": "true"}, source_file_path)
source = FileSource(source_context)

hudi_sink_context = HudiSinkContext(
    spark_write_mode,
    table_name,
    primary_key_field,
    parition_column,
    "upsert",
    precombine_field,
    target_file_path
)
hudi_sink = HudiSink(hudi_sink_context)


def transform(df: DataFrame) -> DataFrame:
    timestamp_columns = ["created_date", "updated_date"]
    w_create_ts = "2023-01-04 00:00:00"
    for column in timestamp_columns:
        df = df.withColumn(column, f.col(column).cast("timestamp"))

    return (
        df.withColumn(parition_column, f.date_format(f.col(partition_from), "yyyyMMdd"))
        .withColumn("w_create_ts", f.lit(w_create_ts).cast("timestamp"))
    )

spark = (
    SparkSession.Builder()
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.2-bundle_2.12:0.12.2")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .appName("Local app")
    .master("local")
    .getOrCreate()
)

ingestion_context = IngestionContext(
    source,
    transform,
    hudi_sink
)

ingestion_pipeline = IngestionPipeline(ingestion_context)
ingestion_pipeline.run(spark)