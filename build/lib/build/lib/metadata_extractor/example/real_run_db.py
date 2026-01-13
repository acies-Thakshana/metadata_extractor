import os
from dotenv import load_dotenv

from metadata_extractor import extract_metadata

load_dotenv()

db_conn = os.getenv("DB_CONN")
if not db_conn:
    raise ValueError("DB_CONN not found in .env file")

metadata = extract_metadata(
    db_conn=db_conn,
    output_path="output/metadata_catalog.json"
)

print("DONE")