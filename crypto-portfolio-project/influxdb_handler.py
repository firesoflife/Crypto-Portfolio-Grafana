# New file with changes highlighted and comments on removed lines

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS


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
            url=websocket_url, token=token, org=org
        )
        self.ohlc_client = influxdb_client.InfluxDBClient(
            url=ohlc_url, token=token, org=org
        )

        # Separate write APIs for two buckets
        self.ws_write_api = self.ws_client.write_api(write_options=SYNCHRONOUS)
        self.ohlc_write_api = self.ohlc_client.write_api(
            write_options=SYNCHRONOUS)

    # WebSocket Price Updates
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

    # OHLC Writing Logic
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

    # Ticker Data Storage (Augmented Feature for Step 2)
    # highlight-next-line
    def write_ticker_data(self, currency_pair, ticker_data, timestamp, logo_url=None):
        """
        Write ticker data to InfluxDB.

        Args:
            currency_pair (str): The currency pair, e.g., "btcusd".
            ticker_data (dict): The ticker data, including fields like open, high, low, last, volume, etc.
            timestamp (int): The UNIX timestamp in nanoseconds.
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

            # Add logo field if available
            if logo_url:
                point = point.tag("logo_url", logo_url)

                # Write the point to the OHLC bucket
            self.ohlc_write_api.write(bucket="crypto_ticker", record=point)
            print(
                f"Ticker data written for {currency_pair}: {timestamp} with logo {logo_url} ")
        except Exception as e:
            print(f"Error writing ticker data to InfluxDB: {e}")

    # Query Logic (Unmodified for Historical Data)
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
