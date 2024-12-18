
import asyncio
import json
import os
from dotenv import load_dotenv
from influxdb_handler import InfluxDBHandler
from websocket_client import WebSocketClient
from http_handler import HTTPHandler

# --- ENVIRONMENT AND CONFIGURATION ---

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# InfluxDB Configuration
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')

# Bitstamp Configurations
CURRENCY_PAIRS = ["btcusd", "xrpusd", "xlmusd",
                  "hbarusd", "vetusd", "csprusd", "xdcusd"]
WS_URL = "wss://ws.bitstamp.net"
HTTP_BASE_URL = "https://www.bitstamp.net/api/v2"  # Base URL for Bitstamp HTTP API

# --- MODULE INITIALIZATIONS ---
influxdb_handler = InfluxDBHandler(
    websocket_url=INFLUXDB_URL,
    ohlc_url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG,
)

# Initialize the HTTP handler
http_handler = HTTPHandler(base_url=HTTP_BASE_URL)

# --- PROCESS LIVE TRADE DATA ---


async def process_message(message):
    try:
        message_json = json.loads(message)
        event = message_json.get("event")

        if event == "trade":
            data = message_json.get("data")
            currency_pair = message_json["channel"].split("_")[2]
            price = float(data["price"])
            # InfluxDB requires nanoseconds
            timestamp = int(data["timestamp"]) * 1000000000

            # Write live WebSocket trade data to InfluxDB
            influxdb_handler.write_data(currency_pair, price, timestamp)

    except Exception as e:
        print(f"Error in process_message: {e}")


# --- BACKFILL HISTORICAL DATA ---
async def backfill_ohlc(currency_pair):
    """
    Fetch and backfill historical OHLC data into InfluxDB for a currency pair.
    """
    print(f"Backfilling OHLC data for {currency_pair}...")
    try:
        ohlc_data = http_handler.fetch_ohlc(
            currency_pair=currency_pair,
            step=3600,  # Timeframe of 1-hour candles
            limit=1000  # Fetch up to 1000 data points
        )

        for candle in ohlc_data:
            influxdb_handler.write_ohlc_data(
                currency_pair=currency_pair,
                open_=float(candle["open"]),
                high=float(candle["high"]),
                low=float(candle["low"]),
                close=float(candle["close"]),
                volume=float(candle["volume"]),
                timestamp=int(candle["timestamp"]) * 1000000000  # Nanoseconds
            )
        print(f"Finished backfilling OHLC data for {currency_pair}.")
    except Exception as e:
        print(f"Error fetching OHLC data for {currency_pair}: {e}")


# --- MAIN FUNCTION ---
async def main():
    """
    Main function to perform OHLC backfill first, then start real-time WebSocket streaming.
    """
    print("Starting historical backfill...")
    for pair in CURRENCY_PAIRS:
        # Sequentially backfill historical data for each pair
        await backfill_ohlc(pair)

    print("Switching to real-time WebSocket listener...")
    ws_client = WebSocketClient(url=WS_URL, currency_pairs=CURRENCY_PAIRS)
    await ws_client.listen(process_message)  # Start WebSocket listener


if __name__ == "__main__":
    asyncio.run(main())
