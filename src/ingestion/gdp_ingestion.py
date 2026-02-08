# Import libraries
import json
import uuid

from sqlalchemy import text

from config.api_endpoints import ENDPOINTS
from config.database import get_engine
from src.ingestion.api_client import APIClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


def ingest_gdp_data() -> str:
    client = APIClient()
    engine = get_engine()
    batch_id = str(uuid.uuid4())

    try:
        logger.info("Fetching GDP data from API...")
        data, status_code = client.fetch(ENDPOINTS["gdp_quarterly"])

        # Store raw data in bronze layer
        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO bronze.gdp_raw
                    (api_endpoint, response_status, raw_data,
                     row_count, ingestion_batch_id)
                    VALUES (:endpoint, :status, :data,
                            :count, :batch_id)
                    """),
                {
                    "endpoint": ENDPOINTS["gdp_quarterly"],
                    "status": status_code,
                    "data": json.dumps(data),
                    "count": len(data) if isinstance(data, list) else 1,
                    "batch_id": batch_id,
                },
            )
            conn.commit()

        logger.info(
            "Ingested %d GDP records with batch_id: %s",
            len(data) if isinstance(data, list) else 1,
            batch_id,
        )
        return batch_id
    except Exception:
        logger.exception("GDP ingestion failed for batch_id: %s", batch_id)
        raise


if __name__ == "__main__":
    ingest_gdp_data()
