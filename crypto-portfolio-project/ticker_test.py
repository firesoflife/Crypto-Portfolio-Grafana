import requests


class HTTPHandler:
    def __init__(self, base_url):
        self.base_url = base_url

    def fetch_ticker_info(self, currency_pairs):
        tickers = {}
        for pair in currency_pairs:
            # Correct endpoint
            url = f"{self.base_url}/ticker/{pair}/"
            response = requests.get(url)

            if response.status_code == 200:
                # Adjust this based on the returned JSON format
                tickers[pair] = response.json()  # Get full response directly
            else:
                print(
                    f"Failed to fetch ticker data for {pair}: {response.status_code}, {response.text}")

        return tickers


def test_fetch_ticker_info():
    # Correct base URL
    http_handler = HTTPHandler(base_url="https://www.bitstamp.net/api/v2")
    currency_pairs = ["btcusd", "xrpusd", "xlmusd",
                      "hbarusd", "vetusd", "csprusd", "xdcusd"]
    ticker_info = http_handler.fetch_ticker_info(currency_pairs)

    print("Ticker Information:")
    for pair, data in ticker_info.items():
        print(f"{pair}: {data}")


if __name__ == "__main__":
    test_fetch_ticker_info()
