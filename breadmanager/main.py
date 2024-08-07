import os  # noqa: F401
import logging
from dotenv import load_dotenv
from database import create_db_connection, init_db, create_postgres_table


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Load environment variables from .env file
    load_dotenv()

    # Database initialization
    pg_conn = create_db_connection()
    logging.info("Initializing database...")
    init_db(pg_conn, "finance")
    logging.info("Database initialization complete.")
    logging.info("Closing database connection")
    pg_conn.close()

    # Create hypertables for each security
    conn = create_db_connection(database="finance")
    logging.info("Creating hypertable")
    create_postgres_table(conn, "market_data", "tslatest1y")
    conn.close()


if __name__ == "__main__":
    main()
