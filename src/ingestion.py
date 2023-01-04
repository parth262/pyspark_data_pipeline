from dataset.source import *
from dataset.sink import *
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from typing import Callable
from dataclasses import dataclass

Transformation = Callable[[DataFrame], DataFrame]

@dataclass
class IngestionContext:
    source: Source
    transformation: Transformation
    sink: Sink


class IngestionPipeline:

    def __init__(self, ingestion_context: IngestionContext) -> None:
        self.context = ingestion_context

    def run(self, spark: SparkSession):
        df = self.context.source.read(spark)
        processed_df = self.context.transformation(df)
        self.context.sink.write(processed_df)
