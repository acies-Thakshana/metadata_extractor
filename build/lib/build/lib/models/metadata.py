from dataclasses import dataclass
from typing import List, Optional

@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    samples: List[str]
    unique_count: int
    min: Optional[float]
    max: Optional[float]
    mean: Optional[float]
    pattern: Optional[str]
    cardinality_ratio: float
    business_name: Optional[str] = ""
    description: Optional[str] = ""
    role: Optional[str] = ""
    enum_like: bool = False
    is_primary_key: bool = False
    is_join_key: bool = False
    related_entities: List[str] = None

@dataclass
class EdgeMetadata:
    table: str
    column: str
    parent_table: str
    parent_column: str
    match_type: str
    match_ratio: float
    name_match_ratio: float = 0.0
    data_match_ratio: float = 0.0
    final_ratio: float = 0.0
    rel_type: str = ""

@dataclass
class TableMetadata:
    table: str
    total_rows: int
    total_columns: int
    columns: List[ColumnMetadata]
    edges: List[EdgeMetadata]
