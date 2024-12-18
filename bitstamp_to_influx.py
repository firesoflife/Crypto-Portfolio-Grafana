import asyncio
import websockets
import json
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# --- CONFIGURATION ---
# Websocket URL
WS_URL = "wss://ws.bitstamp.net"

# InfluxDB configuration
INFLUXDB_URL = 'http://10.2.1.185:8086'
INFLUXDB_TOKEN = 'gcTaI_Z_Ffra5nJ51hk71KZHHQNNSYWZ3E6h0xU_Po0nQIdSQNhIKdYOzrJCwjCpSSHRqsv7bb_HZ3ciyiPdnw=='
INFLUXDB_ORG = 'bglab'
INFLUXDB_BUCKET = 'crypto_portfolio'

# Currency pairs to subscribe
CURRENCY_PAIRS = ["btcusd", "xrpusd",
                  "xlmusd", "hbarusd", "vetusd", "csprusd", "xdcusd"]

# --- INITIALIZE INFLUXDB CLIENT ---

client = influxdb_client.InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)
write_api = client.write_api(write_options=SYNCHRONOUS)

# --- WEBSOCKET FUNCTIONS ---


async def process_message(message):
    """
    Handle incoming Websocket message and write live trade prices to InfluxDB
    """
    try:
        # Parse Websocket message
        message_data = json.loads(message)
        event = message_data.get("event")
        data = message_data.get("data")

        # Process trade data
        if event == "trade":
            # Extract relevant info
            currency_pair = message_data["channel"].split("_")[2]
            price = float(data["price"])
            timestamp = int(data["timestamp"]) * 1000000000

            # Write to InfluxDB
            point = influxdb_client.Point("crypto_data") \
                .tag("currency_pair", currency_pair) \
                .field("price", price) \
                .time(timestamp)

            write_api.write(bucket=INFLUXDB_BUCKET,
                            org=INFLUXDB_ORG, record=point)

            # Print to console
            print(f"[{currency_pair}] Spot Price: {price} USD @ {timestamp}")
    except Exception as e:
        print(f"Error processing message: {e}")


async def subscribe_to_pairs(websocket, pairs):
    """
    Subscribe to Websocket 'live_trades' channel for each currency pair
    """
    for pair in pairs:
        # Build subscription message
        subscription_message = {
            "event": "bts:subscribe",
            "data": {
                "channel": f"live_trades_{pair}"
            }
        }
        await websocket.send(json.dumps(subscription_message))
        print(f"Subscribed to channel: live_trades_{pair}")


async def main():
    """
    Main Websocket connection handler for subscribing to Bitstamp API currency pairs and processing messages
    """
    async with websockets.connect(WS_URL) as websocket:
        # Subscribe to currency pairs
        await subscribe_to_pairs(websocket, CURRENCY_PAIRS)

        # Listen to Websocket messages continuously
        while True:
            message = await websocket.recv()
            await process_message(message)

# --- RUN MAIN ---
if __name__ == "__main__":
    asyncio.run(main())
