from http_handler import HTTPHandler
from influxdb_handler import InfluxDBHandler
import time
import os
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# **InfluxDB Configuration**
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')


def test_ticker_to_influxdb():
    """
    Test writing ticker data directly into InfluxDB with logos.
    """

    # Initialize the HTTPHandler
    http_handler = HTTPHandler(
        base_url="https://www.bitstamp.net/api/v2",
        tracked_currency_pairs=["btcusd", "xrpusd"]
    )

    # Fetch logos and metadata for the tracked coins
    filtered_currencies, all_currencies, unmatched_pairs = http_handler.fetch_currencies_with_logo()
    logos = {currency["currency"].upper(): currency["logo"]
             for currency in filtered_currencies}

    # Example ticker data (static for testing)
    ticker_info = {
        "btcusd": {
            "open": "27100.00",
            "high": "27300.00",
            "low": "26950.00",
            "last": "27200.00",
            "volume": "120.5",
        },
        "xrpusd": {
            "open": "0.50000",
            "high": "0.51000",
            "low": "0.48000",
            "last": "0.49250",
            "volume": "543211.2"
        }
    }

    # Initialize the InfluxDBHandler
    influxdb_handler = InfluxDBHandler(
        websocket_url=INFLUXDB_URL,
        ohlc_url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    )

    # Write the ticker data to InfluxDB (including logos)
    for pair, data in ticker_info.items():
        # Convert current time to nanoseconds for InfluxDB
        timestamp = int(time.time() * 1e9)

        # Extract logo URL for the base currency (e.g., "BTC" from "btcusd")
        base_currency = pair[:-3].upper()
        logo_url = logos.get(base_currency)  # Fallback to None if not found

        influxdb_handler.write_ticker_data(
            pair, data, timestamp, logo_url=logo_url)


if __name__ == "__main__":
    test_ticker_to_influxdb()
