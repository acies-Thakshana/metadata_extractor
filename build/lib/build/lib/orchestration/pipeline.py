from sqlalchemy import text
from metadata_extractor.models.metadata import TableMetadata
from metadata_extractor.extractors.schema import SchemaInspector
from metadata_extractor.extractors.profiling import TableProfiler
from metadata_extractor.extractors.matching import DataMatcher
from metadata_extractor.extractors.relationships import RelationshipDetector
from metadata_extractor.extractors.enrichment import ConstraintRelationshipEnricher

class MetadataExtractor:
    def __init__(self, db_conn):
        self.inspector = SchemaInspector(db_conn)
        self.profiler = TableProfiler(self.inspector.engine)
        self.matcher = DataMatcher(self.inspector.engine)
        self.detector = RelationshipDetector(self.matcher)
        self.constraint_enricher = ConstraintRelationshipEnricher(self.inspector.engine)

    def run(self):
        tables = []

        for (_, table), cols in self.inspector.get_all_columns().items():
            total_rows = self._count(table)
            columns = [
                self.profiler.profile_column(table, c["name"], str(c["type"]), total_rows)
                for c in cols
            ]

            tables.append(
                TableMetadata(
                    table=table,
                    total_rows=total_rows,
                    total_columns=len(columns),
                    columns=columns,
                    edges=[]
                )
            )

        self.constraint_enricher.enrich(tables)

        inferred = self.detector.detect(tables)
        for t in tables:
            t.edges.extend(e for e in inferred if e.table == t.table)

        return tables

    def _count(self, table):
        with self.profiler.engine.connect() as conn:
            return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
