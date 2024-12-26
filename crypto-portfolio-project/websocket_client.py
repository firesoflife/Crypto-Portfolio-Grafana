import asyncio
import json
import websockets


class WebSocketClient:
    def __init__(self, url, currency_pairs):
        """
        Initialize the WebSocket client.

        Args:
            url (str): WebSocket server URL.
            currency_pairs (list): List of currency pairs to subscribe to.
        """
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
            try:
                await websocket.send(json.dumps(subscription_message))
                print(f"[INFO] Subscribed to {pair}")
            except Exception as e:
                print(f"[ERROR] Failed to subscribe to {pair}: {e}")

    async def listen(self, message_handler):
        """
        Connect to the WebSocket, subscribe to pairs, and listen for messages.
        """
        while True:
            try:
                # Establish a connection to the WebSocket server
                async with websockets.connect(self.url, ping_interval=None) as websocket:
                    print("[INFO] WebSocket connection established.")
                    await self.subscribe_to_pairs(websocket)

                    # Receive messages and pass them to the handler
                    while True:
                        message = await websocket.recv()
                        await message_handler(message)

            except websockets.exceptions.ConnectionClosed as e:
                print(
                    f"[ERROR] WebSocket connection closed: {e}. Reconnecting...")
            except asyncio.TimeoutError:
                print("[ERROR] WebSocket connection timed out. Reconnecting...")
            except Exception as e:
                print(
                    f"[ERROR] Unexpected WebSocket error: {e}. Reconnecting...")
            finally:
                print("[INFO] Reconnecting to WebSocket in 5 seconds...")
                await asyncio.sleep(5)
