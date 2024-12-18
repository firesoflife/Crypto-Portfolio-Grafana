import asyncio
import json
import websockets


class WebSocketClient:
    def __init__(self, url, currency_pairs):
        self.url = url
        self.currency_pairs = currency_pairs

    async def subscribe_to_pairs(self, websocket):
        """
        Subscribe to specific currency pairs.
        """
        for pair in self.currency_pairs:
            subscription_message = {
                "event": "bts:subscribe",
                "data": {"channel": f"live_trades_{pair}"}
            }
            await websocket.send(json.dumps(subscription_message))
            print(f"Subscribed to {pair}")

    async def listen(self, message_handler):
        """
        Connect to the WebSocket, subscribe to pairs, and listen for messages.
        """
        async with websockets.connect(self.url) as websocket:
            await self.subscribe_to_pairs(websocket)

            while True:
                message = await websocket.recv()
                await message_handler(message)
