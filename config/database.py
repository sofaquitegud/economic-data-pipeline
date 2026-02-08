# Import libraries
import os
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Initiate env file
load_dotenv()


# Main function
def get_engine() -> Engine:
    """Database connection"""
    user = os.getenv("POSTGRES_USER", "")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "malaysia_data")

    conn_string = "postgresql://{}:{}@{}:{}/{}".format(
        quote_plus(user),
        quote_plus(password),
        host,
        port,
        db,
    )

    return create_engine(
        conn_string,
        pool_pre_ping=True,
        connect_args={"sslmode": "prefer"},
        future=True,
    )
