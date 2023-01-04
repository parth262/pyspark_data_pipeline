from pyspark.sql.session import SparkSession


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

input_path = "C:/Users/parth/Desktop/workspace/data_pipeline/output/users"

df = (spark.read.format("hudi")
    .load(input_path))

df.printSchema()

result = df.groupBy("last_modified").count()

result.show()