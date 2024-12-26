import requests


class HTTPHandler:
    def __init__(self, base_url, tracked_currency_pairs):
        """
        Initialize the HTTP client for Bitstamp API.
        Args:
            base_url (str): Base URL for Bitstamp API.
            tracked_currency_pairs (list): List of tracked currency pairs (e.g., ["btcusd", "xrpusd"]).
        """
        self.base_url = base_url
        self.tracked_currency_pairs = tracked_currency_pairs

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

    def fetch_currencies_with_logo(self):
        """
        Fetch a list of all available currencies with their logos and filter them.

        Returns:
            tuple: (filtered_currencies: list, all_currencies: list, unmatched_pairs: list)
                - filtered_currencies: Only the coins that match your tracked pairs.
                - all_currencies: Full list of currencies from the Bitstamp API.
                - unmatched_pairs: A list of pairs where no matching symbol was found in the response.
        """
        url = f"{self.base_url}/currencies/"
        response = requests.get(url)

        if response.status_code != 200:
            print(
                f"Failed to fetch currencies: {response.status_code}, {response.text}")
            return [], [], []

        all_currencies = response.json()

        # Get unique symbols (e.g., ["BTC", "XRP"]) from tracked pairs (e.g., ["btcusd", "xrpusd"])
        tracked_symbols = set(pair[:-3].upper()
                              for pair in self.tracked_currency_pairs)

        # Filter the currencies based on tracked symbols
        filtered_currencies = [
            currency for currency in all_currencies if currency["currency"].upper() in tracked_symbols
        ]

        # Identify pairs that didn't match any symbol in /currencies/
        matched_symbols = {currency["currency"].upper()
                           for currency in all_currencies}
        unmatched_pairs = [
            pair for pair in self.tracked_currency_pairs if pair[:-3].upper() not in matched_symbols
        ]

        return filtered_currencies, all_currencies, unmatched_pairs
