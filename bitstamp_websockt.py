import asyncio
import websockets
import json


CURRENCY_PAIRS = ["btcusd", "ethusd",
                  "xrpusd", "xlmusd", "hbarusd", "vetusd", "csprusd", "xdcusd"]
# Bitstamp websocket API URL
WS_URL = "wss://ws.bitstamp.net"

# Store latest price
latest_price = {}


async def process_message(message):
    """
    Process a Websocket message and update the latest price
    """
    global latest_price

    message_data = json.loads(message)
    event = message_data.get("event")
    data = message_data.get("data")

    if event == "trade":
        currency_pair = message_data["channel"].split("_")[2]
        price = float(data["price"])

        # Update in memory latest price
        latest_price[currency_pair] = price

        print(f"[{currency_pair}] Spot Price: = {price} USD")


async def subscribe_to_pairs(websocket, pairs):
    """
    Subscribe to the 'live_trades' channel for each currency pair
    """
    for pair in pairs:
        subscription_message = {
            "event": "bts:subscribe",
            "data": {
                "channel": f"live_trades_{pair}"
            }
        }
        await websocket.send(json.dumps(subscription_message))
        print(f"Subscribed to live_trades_{pair} channel")


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

if __name__ == "__main__":
    asyncio.run(main())
