import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
from ib_async import IB, Stock
from breadmanager import (
    create_db_connection,
    create_postgres_table,
    get_earliest_record,
    get_latest_record,
    write_dataframe_to_postgres,
    generate_contract_table_name,
    get_historical_df,
    get_secret,
    date_range_generator,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

secret_name = "FinanceInfrastructureStackD-Wl3bOyaNTOFH"
secret = get_secret(secret_name)

# Database connection parameters
db_params = {
    "host": secret["host"],
    "port": secret["port"],
    "database": secret["dbname"],
    "user": secret["username"],
    "password": secret["password"],
}
conn = create_db_connection(**db_params)

try:
    ib = IB()
    ib.connect("127.0.0.1", 4001, clientId=1)
except Exception as e:
    logging.error(e)
    logging.error("failed")

contracts = [
    Stock("AAPL", "NASDAQ", "USD"),
    Stock("MSFT", "NASDAQ", "USD"),
    Stock("LUV", "NYSE", "USD"),
    Stock("SHEL", "NYSE", "USD"),
    Stock("WMT", "NYSE", "USD"),
]

now = datetime.now(timezone.utc)
timedelta_of_data_required = timedelta(
    days=60
)  # Starts pull from 30 days before the delta due to durationStr
schema_name = "market_data"


for contract in contracts:
    logging.info(f"{contract.symbol}: getting data")
    table_name = generate_contract_table_name(contract, "1 min")
    # Only creates table if it does not exist
    create_postgres_table(conn, schema_name, table_name)

    earliest_record = get_earliest_record(conn, schema_name, table_name)
    # Check if data exists in the table & if earliest data is at least "days_of_data_required" days old
    if (earliest_record is None) or (
        earliest_record["timestamp"] > now - timedelta_of_data_required
    ):
        dfs = []
        start = now - timedelta_of_data_required
        for interval in date_range_generator(start, now, timedelta_of_data_required):
            logging.info(
                f"{contract.symbol}: pulling data with endDateTime: {interval}"
            )
            dfs.append(
                get_historical_df(
                    ib,
                    contract,
                    endDateTime=interval,
                    durationStr="30 D",
                    barSizeSetting="1 min",
                    whatToShow="TRADES",
                    useRTH=True,
                    formatDate=1,
                )
            )
        df = pd.concat(dfs)
        df = df[~df.index.duplicated(keep="first")]
        write_dataframe_to_postgres(df, conn, schema_name, table_name)

    else:
        # latest_record should not be None.
        latest_record = get_latest_record(conn, schema_name, table_name)
        latest_delta = now - latest_record["timestamp"]
        logging.info(
            f"{contract.symbol}: latest data is within the last {latest_delta.days + 1} day(s)"
        )
        # TODO: Handle this occurence (probably include this case in the above block)
        if latest_record["timestamp"] < now - timedelta(days=30):
            raise Exception("latest data not pulled for over a month")

        logging.info(
            f"{contract.symbol}: getting data for the last {latest_delta.days + 1} day(s)"
        )
        df = get_historical_df(
            ib,
            contract,
            endDateTime="",
            durationStr=f"{latest_delta.days + 1} D",
            barSizeSetting="1 min",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )
        df = df[~df.index.duplicated(keep="first")]
        write_dataframe_to_postgres(df, conn, schema_name, table_name)

ib.disconnect()
logging.info("Script complete")
