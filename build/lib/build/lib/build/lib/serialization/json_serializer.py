import json
from datetime import datetime

class MetadataSerializer:
    @staticmethod
    def to_dict(tables, database_name):
        return {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "database": database_name,
            "tables": [MetadataSerializer._table(t) for t in tables]
        }

    @staticmethod
    def _table(t):
        return {
            "table": t.table,
            "total_rows": t.total_rows,
            "columns": [vars(c) for c in t.columns],
            "edges": [vars(e) for e in t.edges]
        }

    @staticmethod
    def write(payload, path):
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, default=str)
