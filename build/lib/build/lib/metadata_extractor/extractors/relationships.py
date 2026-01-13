from rapidfuzz import fuzz
from metadata_extractor.models.metadata import EdgeMetadata

class RelationshipDetector:
    NAME_THRESHOLD = 0.85
    DATA_THRESHOLD = 0.10

    def __init__(self, data_matcher):
        self.data_matcher = data_matcher

    def detect(self, tables):
        edges = []
        for t1 in tables:
            for c1 in t1.columns:
                for t2 in tables:
                    if t1.table == t2.table:
                        continue
                    for c2 in t2.columns:
                        if c1.data_type != c2.data_type:
                            continue

                        name_score = fuzz.ratio(c1.name, c2.name) / 100
                        data_score = 0.0

                        if name_score >= self.NAME_THRESHOLD:
                            data_score = self.data_matcher.match_ratio(
                                t1.table, c1.name, t2.table, c2.name
                            )

                        if name_score >= self.NAME_THRESHOLD or data_score >= self.DATA_THRESHOLD:
                            edges.append(
                                EdgeMetadata(
                                    table=t1.table,
                                    column=c1.name,
                                    parent_table=t2.table,
                                    parent_column=c2.name,
                                    match_type="inferred",
                                    match_ratio=name_score,
                                    name_match_ratio=name_score,
                                    data_match_ratio=data_score,
                                    final_ratio=max(name_score, data_score),
                                    rel_type="UNK"
                                )
                            )
        return edges
