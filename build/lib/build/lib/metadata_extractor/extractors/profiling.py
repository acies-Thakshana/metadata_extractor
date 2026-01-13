import re
import pandas as pd
from sqlalchemy import text
from metadata_extractor.utils.sampling import DataSampler
from metadata_extractor.models.metadata import ColumnMetadata


class TableProfiler:
    LOW_CARDINALITY_THRESHOLD = 0.05

    # Types that CANNOT be aggregated with MIN / MAX
    NON_COMPARABLE_TYPES = (
        "json",
        "jsonb",
        "uuid",
        "array",
    )

    def __init__(self, engine):
        self.engine = engine
        self.sampler = DataSampler(engine)

    def profile_column(self, table, column, dtype, total_rows):
        dtype_lower = dtype.lower()

        is_string = "char" in dtype_lower or "text" in dtype_lower
        is_non_comparable = any(t in dtype_lower for t in self.NON_COMPARABLE_TYPES)

        # ----------------------------
        # Sampling (unchanged logic)
        # ----------------------------
        samples = (
            self.sampler.sample_uniform(table, column)
            if self._is_low_cardinality(table, column)
            else self.sampler.sample_distinct(table, column)
        )

        # ----------------------------
        # SQL generation (SAFE)
        # ----------------------------
        if is_non_comparable:
            sql = f"""
            SELECT
                COUNT(DISTINCT {column}) AS unique_count,
                NULL AS min_val,
                NULL AS max_val,
                NULL AS avg_val
            FROM {table}
            """
        elif is_string:
            sql = f"""
            SELECT
                COUNT(DISTINCT {column}) AS unique_count,
                MIN(LENGTH({column})) AS min_val,
                MAX(LENGTH({column})) AS max_val,
                AVG(LENGTH({column})) AS avg_val
            FROM {table}
            WHERE {column} IS NOT NULL
            """
        else:
            sql = f"""
            SELECT
                COUNT(DISTINCT {column}) AS unique_count,
                MIN({column}) AS min_val,
                MAX({column}) AS max_val,
                0 AS avg_val
            FROM {table}
            """

        # ----------------------------
        # Execute SQL
        # ----------------------------
        stats = pd.read_sql(sql, self.engine).iloc[0]

        # ----------------------------
        # Pattern detection
        # ----------------------------
        pattern = self._detect_pattern(samples)

        # ----------------------------
        # Build metadata object
        # ----------------------------
        return ColumnMetadata(
            name=column,
            data_type=dtype,
            samples=samples,
            unique_count=int(stats.unique_count) if stats.unique_count is not None else 0,
            min=stats.min_val,
            max=stats.max_val,
            mean=stats.avg_val,
            pattern=pattern,
            cardinality_ratio=(
                stats.unique_count / max(total_rows, 1)
                if stats.unique_count is not None
                else 0.0
            ),
        )

    def _is_low_cardinality(self, table, column):
        sql = f"""
        SELECT COUNT(DISTINCT {column})::float / NULLIF(COUNT(*), 0)
        FROM {table}
        WHERE {column} IS NOT NULL
        """
        ratio = self.engine.connect().execute(text(sql)).scalar()
        return ratio is not None and ratio < self.LOW_CARDINALITY_THRESHOLD

    def _detect_pattern(self, samples):
        if not samples:
            return None

        # Numeric-only pattern
        if all(re.fullmatch(r"\d+", str(s)) for s in samples):
            return "INTEGER"

        return "FREE_TEXT"
