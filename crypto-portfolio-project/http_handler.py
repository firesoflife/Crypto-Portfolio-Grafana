import requests


class HTTPHandler:
    def __init__(self, base_url):
        """
        Initialize the HTTP client for Bitstamp API.
        Args:
            base_url (str): Base URL for the Bitstamp API.
        """
        self.base_url = base_url

    def fetch_ohlc(self, currency_pair, step, limit, start=None, end=None):
        """
        Fetch OHLC data for a currency pair.

        Args:
            currency_pair (str): The market symbol, e.g., "btcusd".
            step (int): Timeframe step in seconds (e.g., 3600 for 1-hour candles).
            limit (int): Maximum number of data points to retrieve (max 1000).
            start (int): Start timestamp in Unix time (optional).
            end (int): End timestamp in Unix time (optional).

        Returns:
            list: List of OHLC data points.
        """
        url = f"{self.base_url}/ohlc/{currency_pair}/"

        # Define query parameters for the API call
        params = {
            "step": step,   # OHLC timeframe (e.g., hourly = 3600 seconds)
            "limit": limit  # Max 1000 candles per request
        }
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        # Perform the API request
        response = requests.get(url, params=params)

        if response.status_code == 200:
            ohlc_data = response.json().get("data", {}).get("ohlc", [])
            return ohlc_data
        else:
            raise Exception(
                f"Failed to fetch OHLC data: {response.status_code}, {response.text}")
