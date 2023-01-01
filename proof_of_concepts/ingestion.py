from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f
from datetime import datetime

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

filepath = "resources/users/20221201*.csv"
output_path = "C:/Users/parth/Desktop/workspace/data_pipeline/output/users"

df = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load(filepath)

tableName = "users"

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.partitionpath.field': 'gender',
    'hoodie.datasource.write.table.name': tableName,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'last_modified',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


df = (df
    .where("id>700")
    .withColumn("create_date", f.col("create_date").cast("timestamp"))
    .withColumn("update_date", f.col("update_date").cast("timestamp"))
    # .withColumn("w_create_ts", f.current_timestamp())
    .withColumn("last_modified", f.current_timestamp())
)

df.printSchema()
print(df.count())
df.limit(5).show()
df.write.format("hudi").options(**hudi_options).mode("append").save(output_path)
