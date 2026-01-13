from sqlalchemy import create_engine, inspect

class SchemaInspector:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.inspector = inspect(self.engine)

    def get_all_columns(self, schema: str = "public"):
        return self.inspector.get_multi_columns(schema)
