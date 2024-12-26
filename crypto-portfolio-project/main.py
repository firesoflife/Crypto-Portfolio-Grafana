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

# **InfluxDB Configuration**
INFLUXDB_URL = os.getenv('INFLUXDB_URL')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET')

# **Bitstamp Configuration**
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
http_handler = HTTPHandler(base_url=HTTP_BASE_URL,
                           tracked_currency_pairs=CURRENCY_PAIRS)


# --- UTILITY FUNCTIONS ---

async def get_last_influx_timestamp(currency_pair):
    """
    Query InfluxDB for the last recorded timestamp for a specified currency pair.
    """

    query = f"""
    from(bucket: "crypto_history")
      |> range(start: -1y)
      |> filter(fn: (r) => r._measurement == \"crypto_history\" and r[\"currency_pair\"] == \"{currency_pair}\")
      |> keep(columns: [\"_time\"])
      |> sort(desc: true)
      |> limit(n: 1)
    """

    # Add debug print for the query string
    # ADD: Debug log
    print(f"Generated Flux query for {currency_pair}:\n{query}")

    try:
        result = influxdb_handler.query(query)
        # ADD: Debugging output
        print(f"Query Result for {currency_pair}: {result}")

        if result:
            last_time = result[0]["_time"]
            # Convert to Unix timestamp
            return int(time.mktime(last_time.timetuple()))
        else:
            return None  # Default to None if no result is returned
    except Exception as e:
        print(f"Error querying InfluxDB for {currency_pair}: {e}")
        return None


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
    Avoid duplicates by starting from the last InfluxDB timestamp.
    """
    print(f"Backfilling OHLC data for {currency_pair}...")
    try:
        # Start from the last recorded timestamp
        start = await get_last_influx_timestamp(currency_pair)
        if start is None:
            print(
                f"No existing OHLC data for {currency_pair}, backfilling from the beginning of range.")
        end = int(time.time())  # Current Unix timestamp
        print(f"Fetching {currency_pair} OHLC data from {start} to {end}...")

        # Call the HTTPHandler to fetch OHLC data
        ohlc_data = http_handler.fetch_ohlc(
            currency_pair=currency_pair,
            step=3600,  # Timeframe of 1-hour candles
            limit=1000,  # Fetch maximum 1000 data points
            start=start,  # Dynamic start timestamp
            end=end  # Fetch data up to the current time
        )

        # Write fetched OHLC data into InfluxDB
        for candle in ohlc_data:
            influxdb_handler.write_ohlc_data(
                currency_pair=currency_pair,
                open_=float(candle["open"]),
                high=float(candle["high"]),
                low=float(candle["low"]),
                close=float(candle["close"]),
                volume=float(candle["volume"]),
                timestamp=int(candle["timestamp"]) *
                1000000000  # Convert to nanoseconds
            )
        print(f"Finished backfilling OHLC data for {currency_pair}.")
    except Exception as e:
        print(f"Error fetching OHLC data for {currency_pair}: {e}")


# --- SCHEDULED BACKFILL TASK ---
async def scheduled_backfill():
    """
    Perform OHLC backfills for all currency pairs twice daily.
    """
    while True:
        print("Starting scheduled OHLC backfill task...")
        for pair in CURRENCY_PAIRS:
            await backfill_ohlc(pair)
        print("Scheduled backfill completed. Sleeping for 12 hours.")
        await asyncio.sleep(43200)  # Wait 12 hours (twice daily)


# --- MAIN FUNCTION ---
async def main(manual_backfill):
    """
    Main function to handle both real-time WebSocket streaming and OHLC backfills.
    """
    if manual_backfill:
        print("Manual backfill triggered...")
        for pair in CURRENCY_PAIRS:
            await backfill_ohlc(pair)
        return

    # Start the WebSocket listener for real-time trade data
    print("Starting WebSocket listener...")
    ws_client = WebSocketClient(url=WS_URL, currency_pairs=CURRENCY_PAIRS)
    websocket_task = asyncio.create_task(ws_client.listen(process_message))

    # Start the recurring backfill task for twice-daily updates
    backfill_task = asyncio.create_task(scheduled_backfill())

    # Run both tasks concurrently
    await asyncio.gather(websocket_task, backfill_task)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manual-backfill", help="Trigger a manual OHLC data backfill", action="store_true"
    )
    args = parser.parse_args()

    asyncio.run(main(manual_backfill=args.manual_backfill))
