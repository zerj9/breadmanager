import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from ib_async import IB, Contract, Stock, util


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_arguments():
    parser = argparse.ArgumentParser(description="IB Data Fetcher Service")
    parser.add_argument("client_id", type=int, help="Client ID for IB connection")
    parser.add_argument("ticker", type=str, help="Ticker symbol")
    parser.add_argument(
        "--output_dir",
        type=str,
        default="/path/to/output/directory",
        help="Directory to save output CSV files",
    )
    return parser.parse_args()


def connect_ib(client_id):
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            ib = IB()
            ib.connect("127.0.0.1", 4001, clientId=client_id)
            logging.info(f"Successfully connected to IB on attempt {attempt + 1}")
            return ib
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_attempts - 1:
                logging.info("Retrying in 30 seconds...")
                time.sleep(30)
            else:
                logging.error("Max attempts reached. Exiting.")
                sys.exit(1)


def get_historical_df(ib_object: IB, contract: Contract, **kwargs):
    default_params = {
        "endDateTime": "",
        "durationStr": "1 D",
        "barSizeSetting": "1 min",
        "whatToShow": "TRADES",
        "useRTH": False, # False returns all hours
        "formatDate": 1,
    }
    params = {**default_params, **kwargs}
    bars = ib_object.reqHistoricalData(
        contract,
        endDateTime=params["endDateTime"],
        durationStr=params["durationStr"],
        barSizeSetting=params["barSizeSetting"],
        whatToShow=params["whatToShow"],
        useRTH=params["useRTH"],
        formatDate=params["formatDate"],
    )

    df = util.df(bars).set_index("date")
    df.index.names = ["timestamp"]
    df.columns = ["open", "high", "low", "close", "volume", "average", "bar_count"]
    return df


def generate_contract_table_name(contract, bar_size):
    bars = {"1 min": "1m"}
    name = f"ib_{contract.symbol.lower()}_{contract.exchange.lower()}_{contract.currency.lower()}_{bars[bar_size]}"
    return name


def main():
    args = parse_arguments()
    ib = connect_ib(args.client_id)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    while True:
        try:
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            df = get_historical_df(
                ib,
                Stock(args.ticker, "SMART", "USD"),
                endDateTime="",
                durationStr="1 D",
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )

            output_file = output_dir / f"{args.ticker}_data_{current_time}.csv"
            df.to_csv(output_file)
            logging.info(f"Data saved to {output_file}")

            # Wait before the next fetch
            time.sleep(30)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            logging.info("Attempting to reconnect...")
            ib = connect_ib(args.client_id)


if __name__ == "__main__":
    main()
