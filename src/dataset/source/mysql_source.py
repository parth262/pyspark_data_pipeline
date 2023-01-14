from dataclasses import dataclass, field
from dataset.source.jdbc_source import JDBCSource, JDBCSourceContext
from pyspark.sql import SparkSession, DataFrame


@dataclass
class MySQLSourceContext:
    jdbc_url: str
    table_or_query: str
    username: str
    password: str
    options: dict = field(default_factory=dict)
    fetch_size = 1000


class MySQLSource(JDBCSource):
    def __init__(self, context: MySQLSourceContext):
        jdbc_source_context = JDBCSourceContext(
            "com.mysql.cd.jdbc.Driver", context.jdbc_url, context.table_or_query,
            context.username, context.password, context.options)
        super().__init__(jdbc_source_context)
        self.context = MySQLSourceContext

    def read(self, spark: SparkSession) -> DataFrame:
        return super().read(spark)
