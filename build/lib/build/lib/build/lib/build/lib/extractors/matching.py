import pandas as pd

class DataMatcher:
    def __init__(self, engine):
        self.engine = engine

    def match_ratio(self, t1, c1, t2, c2):
        sql = f"""
        SELECT COUNT(*) AS match_count
        FROM {t1} a
        JOIN {t2} b ON a.{c1} = b.{c2}
        """
        match_count = pd.read_sql(sql, self.engine).iloc[0].match_count
        total = pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {t1}", self.engine).iloc[0].cnt
        return match_count / max(total, 1)
