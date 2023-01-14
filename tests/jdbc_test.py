from dataset.source.jdbc_source import *
from pyspark.sql import SparkSession
from unittest import TestCase


class JDBCSourceTests(TestCase):

    def test_source(self):
        jdbc_source_context = JDBCSourceContext(
            "com.mysql.cj.jdbc.Driver",
            "jdbc:mysql://localhost:3306/test",
            "customers",
            "root",
            "root"
        )

        jdbc_source = JDBCSource(jdbc_source_context)

        spark = (SparkSession.builder
                 .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.31")
                 .master("local")
                 .appName("Local App")
                 .getOrCreate())

        df = jdbc_source.read(spark)

        self.assertEqual(df.count(), 2)
