from sqlalchemy import text
import hashlib

class DataSampler:
    def __init__(self, engine):
        self.engine = engine

    def sample_distinct(self, table, column, limit=10):
        sql = f"""
        SELECT DISTINCT {column}
        FROM {table}
        WHERE {column} IS NOT NULL
        LIMIT :limit
        """
        result = self.engine.connect().execute(text(sql), {"limit": limit})
        return [str(r[0]) for r in result]

    def sample_uniform(self, table, column, limit=25):
        sql = f"""
        SELECT {column}
        FROM {table}
        WHERE {column} IS NOT NULL
        ORDER BY RANDOM()
        LIMIT :limit
        """
        result = self.engine.connect().execute(text(sql), {"limit": limit})
        return [str(r[0]) for r in result]
