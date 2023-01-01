from pyspark.sql.session import SparkSession


def get_spark_with_hudi():
    return (
        SparkSession.Builder()
            .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.2-bundle_2.12:0.12.2")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .appName("Local app")
            .master("local")
            .getOrCreate()
    )


def get_incremental_options(tableName, primaryKeyField, partitionField, precombineField):
    return {
        'hoodie.table.name': tableName,
        'hoodie.datasource.write.recordkey.field': primaryKeyField,
        'hoodie.datasource.write.partitionpath.field': partitionField,
        'hoodie.datasource.write.table.name': tableName,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': precombineField,
    }

def get_full_write_options(tableName, primaryKeyField, partitionField, precombineField):
    return {
        'hoodie.table.name': tableName,
        'hoodie.datasource.write.recordkey.field': primaryKeyField,
        'hoodie.datasource.write.partitionpath.field': partitionField,
        'hoodie.datasource.write.table.name': tableName,
        'hoodie.datasource.write.operation': 'bulk_insert',
        'hoodie.datasource.write.precombine.field': precombineField,
        # 'hoodie.upsert.shuffle.parallelism': 2,
        # 'hoodie.insert.shuffle.parallelism': 2
    }