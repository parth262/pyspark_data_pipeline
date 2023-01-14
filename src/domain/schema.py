from dataclasses import dataclass
from typing import List


@dataclass
class SchemaItem:
    column: str
    data_type: str


Schema = List[SchemaItem]
