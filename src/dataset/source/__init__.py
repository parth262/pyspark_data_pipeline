from abc import abstractmethod
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession


@dataclass
class SourceContext:
    pass


class Source:
    def __init__(self, source_context: SourceContext) -> None:
        self.context = source_context
    
    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        pass
