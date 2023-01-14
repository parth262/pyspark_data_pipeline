from dataset.sink import Sink, SinkContext
from pyspark.sql import DataFrame
from dataclasses import dataclass


@dataclass
class FileSinkContext(SinkContext):
    write_format: str
    options: dict
    filepath: str


class FileSink(Sink):
    def __init__(self, file_sink_context: FileSinkContext):
        super().__init__(file_sink_context)
        self.context = file_sink_context

    def write(self, df: DataFrame):
        (df.write.format(self.context.write_format)
         .options(**self.context.options)
         .mode(self.context.spark_write_mode)
         .save(self.context.filepath))
