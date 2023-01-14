from dataset.source import Source, SourceContext
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame


@dataclass
class JDBCSourceContext(SourceContext):
    driver: str
    jdbc_url: str
    table_or_query: str
    username: str
    password: str
    options: dict = field(default_factory=dict)
    fetch_size = 1000


class JDBCSource(Source):
    def __init__(self, jdbc_source_context: JDBCSourceContext) -> None:
        super().__init__(jdbc_source_context)
        self.context = jdbc_source_context

    def read(self, spark: SparkSession) -> DataFrame:
        return (spark.read.format("jdbc")
                .option("driver", self.context.driver)
                .option("url", self.context.jdbc_url)
                .option("dbtable", self.context.table_or_query)
                .option("user", self.context.username)
                .option("password", self.context.password)
                .option("fetchSize", self.context.fetch_size)
                .options(**self.context.options)
                .load())
