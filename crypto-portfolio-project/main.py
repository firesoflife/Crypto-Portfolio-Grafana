import asyncio
import json
import os
import argparse
import time
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

# Bitstamp Configuration
CURRENCY_PAIRS = ["btcusd", "xrpusd", "xlmusd",
                  "hbarusd", "vetusd", "csprusd", "xdcusd"]
WS_URL = "wss://ws.bitstamp.net"
HTTP_BASE_URL = "https://www.bitstamp.net/api/v2"

# Initialize InfluxDB and HTTP Handlers
influxdb_handler = InfluxDBHandler(
    websocket_url=INFLUXDB_URL,
    ohlc_url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG,
)
http_handler = HTTPHandler(base_url=HTTP_BASE_URL,
                           tracked_currency_pairs=CURRENCY_PAIRS)

# --- FUNCTIONS ---

# Fetch the last recorded timestamp from InfluxDB


async def get_last_influx_timestamp(currency_pair):
    """
    Query InfluxDB for the last recorded timestamp for a specified currency pair.
    """
    query = f"""
    from(bucket: "crypto_history")
      |> range(start: -1y)
      |> filter(fn: (r) => r._measurement == "crypto_history" and r["currency_pair"] == "{currency_pair}")
      |> keep(columns: ["_time"])
      |> sort(desc: true)
      |> limit(n: 1)
    """
    try:
        result = influxdb_handler.query(query)
        if result:
            last_time = result[0]["_time"]
            return int(time.mktime(last_time.timetuple()))
        return None
    except Exception as e:
        print(
            f"Error querying last InfluxDB timestamp for {currency_pair}: {e}")
        return None

# Process WebSocket trade messages and write to InfluxDB


async def process_message(message):
    try:
        message_json = json.loads(message)
        if message_json.get("event") == "trade":
            data = message_json.get("data", {})
            currency_pair = message_json["channel"].split("_")[2]
            price = float(data["price"])
            timestamp = int(data["timestamp"]) * 1_000_000_000
            influxdb_handler.write_data(currency_pair, price, timestamp)
    except Exception as e:
        print(f"Failed to process WebSocket message: {e}")


async def fetch_and_write_ticker_data():
    """
    Fetch ticker data for all configured pairs and write to InfluxDB.
    """
    print("[INFO] Fetching ticker data and metadata for configured pairs...")
    tracked_metadata, _, unmatched_pairs = http_handler.fetch_currencies_with_logo()

    # Warn about unmatched pairs
    if unmatched_pairs:
        print(f"[WARNING] Unmatched currency pairs: {unmatched_pairs}")

    # Map base currencies to their metadata
    metadata_map = {currency["currency"].upper(
    ): currency for currency in tracked_metadata}

    # Fetch ticker data
    ticker_info = http_handler.fetch_ticker_info(CURRENCY_PAIRS)
    if not ticker_info:
        print("[ERROR] No ticker data fetched. Exiting.")
        return  # Exit if no ticker data is retrieved

    for pair, data in ticker_info.items():
        try:
            # Log data before writing to InfluxDB
            print(f"[DEBUG] Writing ticker data for {pair}: {data}")

            timestamp = int(time.time() * 1e9)  # Current time in nanoseconds
            base_currency = pair[:-3].upper()
            metadata = metadata_map.get(base_currency, {})
            influxdb_handler.write_ticker_data(pair, data, timestamp, metadata)
        except Exception as e:
            print(f"[ERROR] Failed to process ticker data for {pair}: {e}")


async def backfill_ohlc(currency_pair):
    try:
        start = await get_last_influx_timestamp(currency_pair)
        if start is None:
            print(
                f"[INFO] No previous data found for {currency_pair}, backfilling from the start.")
        end = int(time.time())
        ohlc_data = http_handler.fetch_ohlc(
            currency_pair, step=3600, limit=1000, start=start, end=end
        )
        for candle in ohlc_data:
            influxdb_handler.write_ohlc_data(
                currency_pair=currency_pair,
                open_=float(candle["open"]),
                high=float(candle["high"]),
                low=float(candle["low"]),
                close=float(candle["close"]),
                volume=float(candle["volume"]),
                timestamp=int(candle["timestamp"]) * 1_000_000_000,
            )
    except Exception as e:
        print(f"[ERROR] Failed to backfill OHLC data for {currency_pair}: {e}")


async def scheduled_fetch():
    """
    Periodically fetch ticker data and backfill historical OHLC data.
    """
    while True:
        print("[INFO] Running scheduled ticker data fetch...")
        await fetch_and_write_ticker_data()  # Fetch and write ticker data

        print("[INFO] Running scheduled OHLC backfill...")
        for pair in CURRENCY_PAIRS:
            await backfill_ohlc(pair)

        print("[INFO] Scheduled fetch completed. Sleeping for 12 hours.")
        await asyncio.sleep(43200)  # 12-hour interval


async def main(manual_backfill, fetch_ticker):
    """
    Main function for periodic tasks or manual commands.
    """
    if manual_backfill:
        print("[INFO] Manual backfill mode activated...")
        for pair in CURRENCY_PAIRS:
            await backfill_ohlc(pair)
        return

    if fetch_ticker:
        print("[INFO] Manual ticker fetch mode activated...")
        await fetch_and_write_ticker_data()
        return

    # Default: Run WebSocket + Scheduled Fetch (OHLC + Ticker)
    print("[INFO] Starting WebSocket listener and scheduled tasks...")
    ws_client = WebSocketClient(url=WS_URL, currency_pairs=CURRENCY_PAIRS)
    websocket_task = asyncio.create_task(ws_client.listen(process_message))
    scheduled_task = asyncio.create_task(scheduled_fetch())
    await asyncio.gather(websocket_task, scheduled_task)

# --- ENTRY POINT ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manual-backfill", action="store_true",
                        help="Trigger manual OHLC data backfill.")
    parser.add_argument("--fetch-ticker", action="store_true",
                        help="Manually fetch ticker data.")
    args = parser.parse_args()
    asyncio.run(main(manual_backfill=args.manual_backfill,
                fetch_ticker=args.fetch_ticker))
