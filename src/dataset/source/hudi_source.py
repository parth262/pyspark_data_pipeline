from dataclasses import dataclass
from dataset.source import Source, SourceContext
from pyspark.sql import SparkSession, DataFrame


@dataclass
class HudiSourceContext(SourceContext):
    filepath: str


class HudiSource(Source):
    def __init__(self, hudi_source_context: HudiSourceContext) -> None:
        super().__init__(hudi_source_context)
        self.context = hudi_source_context

    def read(self, spark: SparkSession) -> DataFrame:
        return spark.read.format("hudi").load(self.context.filepath)
