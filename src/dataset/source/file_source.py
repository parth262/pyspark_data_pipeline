from dataset.source import *
from pyspark.sql import DataFrame, SparkSession
from dataclasses import dataclass


@dataclass
class FileSourceContext(SourceContext):
    file_format: str
    options: dict
    filepath: str


class FileSource(Source):

    def __init__(self, file_source_context: FileSourceContext):
        super().__init__(file_source_context)
        self.context = file_source_context

    def read(self, spark: SparkSession) -> DataFrame:
        return (spark.read.options(**self.context.options)
                .format(self.context.file_format)
                .load(self.context.filepath))
