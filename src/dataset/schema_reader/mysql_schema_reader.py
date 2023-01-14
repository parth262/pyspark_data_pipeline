from dataclasses import dataclass
from pyspark.sql import SparkSession
from dataset.source.jdbc_source import *
from domain.schema import *


@dataclass
class MySQLSchemaReaderContext:
    jdbc_url: str
    database: str
    table: str
    username: str
    password: str


class MySQLSchemaReader:
    def __init__(self, context: MySQLSchemaReaderContext):
        driver = "com.mysql.cj.jdbc.driver"
        sql = f"(select COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS where TABLE_SCHEMA='{context.database}' and TABLE_NAME='{context.table}') as table_schema"
        jdbc_source_context = JDBCSourceContext(
            driver,
            context.jdbc_url, 
            sql,
            context.username,
            context.password
        )
        self.jdbc_source = JDBCSource(jdbc_source_context)

    def read(self, spark: SparkSession) -> Schema:
        result = self.jdbc_source.read(spark).collect()
        return list(map(lambda row: SchemaItem(row.__getitem__("COLUMN_NAME"), row.__getitem__("DATA_TYPE")), result))
