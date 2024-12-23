from influxdb_client.client.write_api import SYNCHRONOUS
import influxdb_client
from http_handler import HTTPHandler
from influxdb_handler import InfluxDBHandler
import time

# Test the fetch method for OHLC data from the Bitstamp API


def test_fetch_currencies_with_logo():
    """
    Test the fetch_currencies_with_logo method to filter only tracked coins with logos.
    """
    # Define tracked currency pairs (this should match your CURRENCY_PAIRS list)
    tracked_currency_pairs = ["btcusd", "xrpusd",
                              "xlmusd", "hbarusd", "vetusd", "csprusd", "xdcusd"]

    # Instantiate the HTTP handler
    http_handler = HTTPHandler(
        base_url="https://www.bitstamp.net/api/v2", tracked_currency_pairs=tracked_currency_pairs)

    # Fetch filtered currencies, all currencies, and unmatched pairs
    filtered_currencies, all_currencies, unmatched_pairs = http_handler.fetch_currencies_with_logo()

    # Print filtered currencies
    print("Filtered Currencies (Tracked only):")
    for currency in filtered_currencies:
        print(
            f"Name: {currency['name']}, Symbol: {currency['currency']}, "
            f"Logo: {currency['logo']}, Type: {currency['type']}"
        )

    # Print unmatched pairs
    print("\nUnmatched Pairs:")
    for pair in unmatched_pairs:
        print(f"{pair} (Base symbol: {pair[:-3].upper()})")


if __name__ == "__main__":
    test_fetch_currencies_with_logo()

# Test the write method for OHLC data to InfluxDB


class InfluxDBHandler:
    def __init__(self, websocket_url, ohlc_url, token, org):
        """
        Initialize the InfluxDB clients for WebSocket and OHLC data buckets.
        Args:
            websocket_url (str): URL for WebSocket InfluxDB bucket.
            ohlc_url (str): URL for OHLC InfluxDB bucket.
            token (str): InfluxDB authentication token.
            org (str): The organization name.
        """
        # Separate clients for WebSocket and OHLC buckets
        self.ws_client = influxdb_client.InfluxDBClient(
            url=websocket_url,
            token=token,
            org=org
        )
        self.ohlc_client = influxdb_client.InfluxDBClient(
            url=ohlc_url,
            token=token,
            org=org
        )

        # Separate write APIs for two buckets
        self.ws_write_api = self.ws_client.write_api(write_options=SYNCHRONOUS)
        self.ohlc_write_api = self.ohlc_client.write_api(
            write_options=SYNCHRONOUS)

    # Ticker Data
    def write_ticker_data(self, currency_pair, ticker_data, timestamp):
        """
        Write ticker data to InfluxDB

        Args:
            currency_pair (str): Currency pair symbol (e.g., "btcusd").
            ticker_data (dict): Ticker data.
            timestamp (int): Unix timestamp.
        """
        try:
            # Build a point for the ticker data
            point = influxdb_client.Point("crypto_ticker") \
                .tag("currency_pair", currency_pair) \
                .field("open", float(ticker_data["open"])) \
                .field("high", float(ticker_data["high"])) \
                .field("low", float(ticker_data["low"])) \
                .field("last", float(ticker_data["last"])) \
                .field("volume", float(ticker_data["volume"])) \
                .time(timestamp)

            # Write to the Crypto Ticker bucket
            self.ohlc_write_api.write(bucket="crypto_ticker", record=point)
            print(f"Ticker data written for {currency_pair}: {timestamp}")
        except Exception as e:
            print(f"Error writing ticker data to InfluxDB: {e}")

    def write_data(self, currency_pair, price, timestamp):
        """
        Write real-time WebSocket price data to InfluxDB (WebSocket bucket).
        """
        try:
            point = influxdb_client.Point("crypto_data") \
                .tag("currency_pair", currency_pair) \
                .field("price", price) \
                .time(timestamp)

            # Write to WebSocket bucket
            self.ws_write_api.write(bucket="crypto_portfolio", record=point)
            print(
                f"Real-time WebSocket data written: {currency_pair} = {price} USD")
        except Exception as e:
            print(f"Failed to write WebSocket data to InfluxDB: {e}")

    def write_ohlc_data(self, currency_pair, open_, high, low, close, volume, timestamp):
        """
        Write OHLC data into InfluxDB (OHLC bucket).
        """
        try:
            point = influxdb_client.Point("crypto_history") \
                .tag("currency_pair", currency_pair) \
                .field("open", open_) \
                .field("high", high) \
                .field("low", low) \
                .field("close", close) \
                .field("volume", volume) \
                .time(timestamp)

            # Write to OHLC bucket
            self.ohlc_write_api.write(bucket="crypto_history", record=point)
            print(f"OHLC data written for {currency_pair}: {timestamp}")
        except Exception as e:
            print(f"Error writing OHLC data to InfluxDB: {e}")


def query(self, query_string):
    """
    Query InfluxDB using Flux and return the results.
    """
    try:
        # Perform the query using the OHLC client
        query_api = self.ohlc_client.query_api()
        tables = query_api.query(query_string)

        # Extract the data from results
        results = []
        for table in tables:
            for record in table.records:
                # Only include _time (and fallback gracefully for other fields)
                results.append(
                    {"_time": record.get_time(), **record.values})

        return results
    except Exception as e:
        print(f"Error querying InfluxDB: {e}")
        return []  # Return an empty list on error
