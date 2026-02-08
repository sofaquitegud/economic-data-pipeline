"""Unit tests for ingestion modules"""

# Import libraries
import os
from unittest.mock import MagicMock, patch

import pytest
import requests
from sqlalchemy import create_engine, text

from src.ingestion.api_client import APIClient


class TestAPIClient:
    """Test API client"""

    def test_init_default_values(self):
        """Test default initialization values"""
        client = APIClient()
        assert client.max_retries == 3
        assert client.retry_delay == 1.0

    def test_init_custom_values(self):
        """Test custom initialization values"""
        client = APIClient(max_retries=5, retry_delay=2.0)
        assert client.max_retries == 5
        assert client.retry_delay == 2.0

    @patch("src.ingestion.api_client.requests.get")
    def test_fetch_success(self, mock_get):
        """Test successful API fetch"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = APIClient()
        data, status_code = client.fetch("http://test.com/api")

        assert data == {"data": "test"}
        assert status_code == 200
        mock_get.assert_called_once()

    @patch("src.ingestion.api_client.requests.get")
    def test_fetch_retry_on_failure(self, mock_get):
        """Test retry logic on failure"""
        mock_success = MagicMock()
        mock_success.json.return_value = {"data": "success"}
        mock_success.status_code = 200
        mock_success.raise_for_status = MagicMock()

        mock_get.side_effect = [
            requests.ConnectionError("Connection error"),
            requests.ConnectionError("Connection error"),
            mock_success,
        ]

        client = APIClient(max_retries=3, retry_delay=0.01)
        data, status_code = client.fetch("http://test.com/api")

        assert data == {"data": "success"}
        assert status_code == 200
        assert mock_get.call_count == 3


class TestIngestionFunctions:
    """Tests for ingestion functions"""

    @patch("src.ingestion.gdp_ingestion.APIClient")
    @patch("src.ingestion.gdp_ingestion.get_engine")
    def test_ingest_gdp_returns_batch_id(self, mock_engine, mock_client):
        """Test GDP ingestion returns a batch ID"""
        mock_client_instance = MagicMock()
        mock_client_instance.fetch.return_value = (
            [{"date": "2024-01", "value": 100}],
            200,
        )
        mock_client.return_value = mock_client_instance

        mock_conn = MagicMock()
        mock_engine.return_value.connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn
        )
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(
            return_value=False
        )

        from src.ingestion.gdp_ingestion import ingest_gdp_data

        batch_id = ingest_gdp_data()

        assert batch_id is not None
        assert len(batch_id) == 36


# ============================================================================
# Integration Tests - Test against real database
# ============================================================================


def get_test_engine():
    """Get database engine for integration tests."""
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/malaysia_data_test",
    )
    return create_engine(db_url)


@pytest.mark.integration
class TestDatabaseIntegration:
    """Integration tests that require a real database connection."""

    def test_database_connection(self):
        """Test that database connection is working."""
        engine = get_test_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            row = result.fetchone()
            assert row[0] == 1

    def test_bronze_schema_exists(self):
        """Test that bronze schema and tables exist."""
        engine = get_test_engine()
        with engine.connect() as conn:
            # Check if bronze schema exists
            result = conn.execute(text("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name = 'bronze'
                    """))
            assert result.fetchone() is not None, "Bronze schema does not exist"

            # Check if required tables exist
            required_tables = [
                "gdp_raw",
                "cpi_raw",
                "labour_raw",
                "exchange_rates_raw",
                "population_raw",
            ]
            for table in required_tables:
                result = conn.execute(text(f"""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'bronze'
                        AND table_name = '{table}'
                        """))
                assert result.fetchone() is not None, f"Table {table} does not exist"

    @patch("src.ingestion.gdp_ingestion.APIClient")
    @patch("src.ingestion.gdp_ingestion.get_engine")
    def test_gdp_ingestion_with_real_db(self, mock_get_engine, mock_client):
        """Test GDP ingestion with real database (mocked API)."""
        # Mock the API call but use real database
        mock_client_instance = MagicMock()
        mock_client_instance.fetch.return_value = (
            [
                {"date": "2024-Q1", "value": 1000.5},
                {"date": "2024-Q2", "value": 1020.3},
            ],
            200,
        )
        mock_client.return_value = mock_client_instance
        mock_get_engine.return_value = get_test_engine()

        from src.ingestion.gdp_ingestion import ingest_gdp_data

        batch_id = ingest_gdp_data()

        # Verify data was inserted
        engine = get_test_engine()
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT ingestion_batch_id, row_count, response_status
                    FROM bronze.gdp_raw
                    WHERE ingestion_batch_id = :batch_id
                    """),
                {"batch_id": batch_id},
            )
            row = result.fetchone()
            assert row is not None, "No data found in database"
            assert str(row[0]) == batch_id
            assert row[1] == 2  # row_count should be 2
            assert row[2] == 200  # status should be 200

            # Clean up test data
            conn.execute(
                text("DELETE FROM bronze.gdp_raw WHERE ingestion_batch_id = :batch_id"),
                {"batch_id": batch_id},
            )
            conn.commit()
