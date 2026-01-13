from metadata_extractor.orchestration.pipeline import MetadataExtractor
from metadata_extractor.serialization.json_serializer import MetadataSerializer


def extract_metadata(
    db_conn: str,
    output_path: str | None = None,
):
    extractor = MetadataExtractor(db_conn=db_conn)
    metadata = extractor.run()  # <-- this is a LIST of tables

    if output_path:
        payload = MetadataSerializer.to_dict(
            tables=metadata,
            database_name=db_conn
        )
        MetadataSerializer.write(payload, output_path)

    return metadata

