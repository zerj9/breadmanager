import csv
import os
import logging
import psycopg2
from io import StringIO
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_db_connection(
    host=None, port=None, user=None, password=None, database="postgres"
):
    db_params = {
        "host": host or os.getenv("DB_HOST", "localhost"),
        "port": port or os.getenv("DB_PORT", "5432"),
        "database": database,
        "user": user or os.getenv("DB_USER", "postgres"),
        "password": password or os.getenv("DB_PASSWORD"),
    }
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error connecting to the database: {error}")
        return None


def init_db(conn, target_db, schema="market_data"):
    try:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn.cursor() as cur:
            # Check if the target database exists
            cur.execute(
                sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"),
                (target_db,),
            )
            db_exists = cur.fetchone()

            # If the database doesn't exist, create it
            if not db_exists:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db))
                )
                logging.info(f"Database '{target_db}' created successfully")
            else:
                logging.info(f"Database '{target_db}' already exists")

        # Connect to the target database
        conn = create_db_connection(database=target_db)

        with conn.cursor() as cur:
            # Check if the schema exists
            cur.execute(
                sql.SQL(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s"
                ),
                (schema,),
            )
            schema_exists = cur.fetchone()

            # If the schema doesn't exist, create it
            if not schema_exists:
                cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                logging.info(f"Schema '{schema}' created successfully")
            else:
                logging.info(f"Schema '{schema}' already exists")

        # Commit the transaction
        conn.commit()

    except (Exception, psycopg2.Error) as error:
        logging.error(f"Error: {error}")

    return conn


def create_postgres_table_query(schema_name, table_name):
    return sql.SQL("""
    CREATE TABLE IF NOT EXISTS {}.{} (
        timestamp TIMESTAMPTZ PRIMARY KEY,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        average DOUBLE PRECISION,
        bar_count INTEGER
    );
    """).format(sql.Identifier(schema_name), sql.Identifier(table_name))


def convert_to_hypertable(schema_name, table_name, interval, time_column="timestamp"):
    return f"""
    DO $$ 
    BEGIN 
        IF NOT EXISTS (
            SELECT 1
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = '{schema_name}'
              AND hypertable_name = '{table_name}'
        ) THEN
            PERFORM create_hypertable('{schema_name}.{table_name}', '{time_column}', chunk_time_interval => interval '{interval}');
        END IF;
    END $$;
    """


def execute_sql(conn, sql_queries):
    try:
        with conn.cursor() as cur:
            for query in sql_queries:
                cur.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"An error occurred: {e}")


def create_schema_if_not_exists(connection, schema_name):
    with connection.cursor() as cursor:
        # Check if the schema exists
        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s;",
            (schema_name,),
        )
        if cursor.fetchone() is None:
            # Schema doesn't exist, so create it
            cursor.execute(f"CREATE SCHEMA {schema_name};")
            connection.commit()
            logging.info(f"Schema '{schema_name}' created successfully.")
        else:
            logging.error(f"Schema '{schema_name}' already exists.")


def create_postgres_table(conn, schema_name, table_name):
    # Generate SQL query for creating a standard PostgreSQL table
    create_table_query = create_postgres_table_query(schema_name, table_name)

    # Check if table exists
    check_table_exists_query = sql.SQL("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = {} AND table_name = {}
    );
    """).format(sql.Literal(schema_name), sql.Literal(table_name))

    try:
        with conn.cursor() as cur:
            cur.execute(check_table_exists_query)
            table_exists = cur.fetchone()[0]

            if table_exists:
                logging.info(f"Table {schema_name}.{table_name} already exists.")
            else:
                # Execute create table query
                cur.execute(create_table_query)
                logging.info(f"Table {schema_name}.{table_name} created successfully.")

        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"An error occurred: {e}")


def create_hypertable(
    conn, schema_name, table_name, interval="1 year", time_column="timestamp"
):
    # Generate SQL queries
    create_table_query = create_postgres_table(schema_name, table_name)
    convert_to_hypertable_query = convert_to_hypertable(
        schema_name, table_name, interval, time_column
    )

    # Check if table exists
    check_table_exists_query = sql.SQL("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = {} AND table_name = {}
    );
    """).format(sql.Literal(schema_name), sql.Literal(table_name))

    try:
        with conn.cursor() as cur:
            cur.execute(check_table_exists_query)
            table_exists = cur.fetchone()[0]

            if table_exists:
                logging.info(f"Table {schema_name}.{table_name} already exists.")
            else:
                # Execute create table query
                cur.execute(create_table_query)
                logging.info(f"Table {schema_name}.{table_name} created successfully.")

            # Execute convert to hypertable query
            cur.execute(convert_to_hypertable_query)
            logging.info(
                f"Hypertable {schema_name}.{table_name} created or confirmed successfully."
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"An error occurred: {e}")


def write_dataframe_to_postgres(df, conn, schema_name, table_name):
    primary_key = df.index.name
    # Reset the index to include it as a column
    df_with_index = df.reset_index()

    # Create a buffer to hold the CSV data
    buffer = StringIO()

    # Write the DataFrame to the buffer as CSV, including the index
    df_with_index.to_csv(buffer, index=False, header=False, quoting=csv.QUOTE_MINIMAL)

    # Move the buffer cursor to the beginning
    buffer.seek(0)

    # Create a cursor object
    cursor = conn.cursor()

    try:
        # Get the column names from the DataFrame (now including the primary key)
        columns = ",".join(df_with_index.columns)

        # Create the temporary table
        cursor.execute(
            f"CREATE TEMP TABLE temp_table (LIKE {schema_name}.{table_name} INCLUDING ALL)"
        )

        # Copy data from buffer to the temporary table
        cursor.copy_expert(f"COPY temp_table({columns}) FROM STDIN WITH CSV", buffer)

        # Prepare and execute the upsert SQL statement
        upsert_sql = f"""
        INSERT INTO {schema_name}.{table_name} ({columns})
        SELECT {columns}
        FROM temp_table
        ON CONFLICT ({primary_key})
        DO UPDATE SET
            {', '.join(f"{col} = EXCLUDED.{col}" for col in df_with_index.columns if col != primary_key)};
        """
        cursor.execute(upsert_sql)

        # Drop the temporary table
        cursor.execute("DROP TABLE temp_table")

        # Commit the transaction
        conn.commit()

        logging.info(
            f"Successfully wrote/updated {len(df)} rows in {schema_name}.{table_name}"
        )

    except Exception as e:
        # If an error occurs, rollback the transaction
        conn.rollback()
        logging.error(f"Error writing to database: {str(e)}")

    finally:
        # Close the cursor
        cursor.close()


def table_exists(connection, schema_name, table_name):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s
                AND table_name = %s
            )
        """,
            (schema_name, table_name),
        )
        return cursor.fetchone()[0]


def get_earliest_record(connection, schema_name, table_name):
    try:
        with connection.cursor() as cursor:
            query = f"""
            SELECT *
            FROM {schema_name}.{table_name}
            ORDER BY timestamp ASC
            LIMIT 1
            """
            cursor.execute(query)
            earliest_record = cursor.fetchone()

            if earliest_record:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, earliest_record))
            else:
                return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        connection.rollback()
        return None


def get_latest_record(connection, schema_name, table_name):
    try:
        with connection.cursor() as cursor:
            query = f"""
            SELECT *
            FROM {schema_name}.{table_name}
            ORDER BY timestamp DESC
            LIMIT 1
            """
            cursor.execute(query)
            latest_record = cursor.fetchone()

            if latest_record:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, latest_record))
            else:
                return None
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        connection.rollback()
        return None
