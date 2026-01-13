from sqlalchemy import inspect
from metadata_extractor.models.metadata import EdgeMetadata

class ConstraintRelationshipEnricher:
    def __init__(self, engine):
        self.inspector = inspect(engine)

    def enrich(self, tables):
        table_lookup = {t.table: t for t in tables}

        for table in tables:
            for fk in self.inspector.get_foreign_keys(table.table):
                parent = fk["referred_table"]
                if parent not in table_lookup:
                    continue

                for c, p in zip(fk["constrained_columns"], fk["referred_columns"]):
                    table.edges.append(
                        EdgeMetadata(
                            table=table.table,
                            column=c,
                            parent_table=parent,
                            parent_column=p,
                            match_type="constraint",
                            match_ratio=1.0,
                            final_ratio=1.0,
                            rel_type="MTO"
                        )
                    )
