from dataset.sink import Sink, SinkContext
from dataclasses import dataclass, field
from pyspark.sql import DataFrame


@dataclass
class JDBCSinkContext(SinkContext):
    driver: str
    jdbc_url: str
    table: str
    username: str
    password: str
    options: dict = field(default_factory=dict)


class JDBCSink(Sink):

    def __init__(self, jdbc_sink_context: JDBCSinkContext) -> None:
        super().__init__(jdbc_sink_context)
        self.context = jdbc_sink_context

    def write(self, df: DataFrame):
        (df.repartition(8).write
            .format("jdbc")
            .option("url", self.context.jdbc_url)
            .option("dbtable", self.context.table)
            .option("user", self.context.username)
            .option("password", self.context.password)
            .options(**self.context.options)
            .mode(self.context.spark_write_mode)
            .save())
