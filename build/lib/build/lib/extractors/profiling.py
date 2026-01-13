import re
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from metadata_extractor.utils.sampling import DataSampler
from metadata_extractor.models.metadata import ColumnMetadata

class TableProfiler:
    LOW_CARDINALITY_THRESHOLD = 0.05

    def __init__(self, engine):
        self.engine = engine
        self.sampler = DataSampler(engine)

    def profile_column(self, table, column, dtype, total_rows):
        is_string = "char" in dtype.lower() or "text" in dtype.lower()

        samples = (
            self.sampler.sample_uniform(table, column)
            if self._is_low_cardinality(table, column)
            else self.sampler.sample_distinct(table, column)
        )

        sql = f"""
        SELECT
            COUNT(DISTINCT {column}) AS unique_count,
            MIN({column}) AS min_val,
            MAX({column}) AS max_val,
            0 AS avg_val
        FROM {table}
        """

        if is_string:
            sql = f"""
            SELECT
                COUNT(DISTINCT {column}) AS unique_count,
                MIN(LENGTH({column})) AS min_val,
                MAX(LENGTH({column})) AS max_val,
                AVG(LENGTH({column})) AS avg_val
            FROM {table}
            WHERE {column} IS NOT NULL
            """

        stats = pd.read_sql(sql, self.engine).iloc[0]

        pattern = self._detect_pattern(samples)

        return ColumnMetadata(
            name=column,
            data_type=dtype,
            samples=samples,
            unique_count=int(stats.unique_count),
            min=stats.min_val,
            max=stats.max_val,
            mean=stats.avg_val,
            pattern=pattern,
            cardinality_ratio=stats.unique_count / max(total_rows, 1)
        )

    def _is_low_cardinality(self, table, column):
        sql = f"""
        SELECT COUNT(DISTINCT {column})::float / NULLIF(COUNT(*),0)
        FROM {table}
        WHERE {column} IS NOT NULL
        """
        ratio = self.engine.connect().execute(text(sql)).scalar()
        return ratio is not None and ratio < self.LOW_CARDINALITY_THRESHOLD

    def _detect_pattern(self, samples):
        if not samples:
            return None
        if all(re.fullmatch(r"\d+", s) for s in samples):
            return "INTEGER"
        return "FREE_TEXT"
