from dataset.source.mysql_source import *
from pyspark.sql import SparkSession
from unittest import TestCase


class JDBCSourceTests(TestCase):

    def test_source(self):
        mysql_source_context = MySQLSourceContext(
            "jdbc:mysql://localhost:3306/test",
            "customers",
            "root",
            "root"
        )

        mysql_source = MySQLSource(mysql_source_context)

        spark = (SparkSession.builder
                 .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.31")
                 .master("local")
                 .appName("Local App")
                 .getOrCreate())

        df = mysql_source.read(spark)

        df.show()

        self.assertEqual(df.count(), 2)
