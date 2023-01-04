from dataset.sink import *
from dataclasses import dataclass
from pyspark.sql import DataFrame


@dataclass
class HudiSinkContext(SinkContext):
    table_name: str
    primary_key_field: str
    parition_field: str
    write_mode: str
    precombine_field: str
    output_path: str


class HudiSink(Sink):

    def __init__(self, hudi_sink_context: HudiSinkContext) -> None:
        super().__init__(hudi_sink_context)
        self.hudi_options = {
            'hoodie.table.name': hudi_sink_context.table_name,
            'hoodie.datasource.write.recordkey.field': hudi_sink_context.primary_key_field,
            'hoodie.datasource.write.partitionpath.field': hudi_sink_context.parition_field,
            'hoodie.datasource.write.table.name': hudi_sink_context.table_name,
            'hoodie.datasource.write.operation': hudi_sink_context.write_mode,
            'hoodie.datasource.write.precombine.field': hudi_sink_context.precombine_field
        }
        self.spark_write_mode = hudi_sink_context.spark_write_mode
        self.sink_path = hudi_sink_context.output_path

    def write(self, df: DataFrame):
        (df.write.format("hudi")
        .options(**self.hudi_options)
        .mode(self.spark_write_mode)
        .save(self.sink_path))