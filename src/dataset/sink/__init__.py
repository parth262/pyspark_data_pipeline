from dataclasses import dataclass
from abc import abstractmethod
from pyspark.sql import DataFrame


@dataclass
class SinkContext:
    spark_write_mode: str


class Sink:
    def __init__(self, sink_context: SinkContext) -> None:
        self.context = sink_context

    @abstractmethod
    def write(self, df: DataFrame):
        pass
    