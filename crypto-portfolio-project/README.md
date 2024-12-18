# Websocket and HTTP API for Crypto Portfolio Project

This project is a simple API that allows you to get the current price of a cryptocurrency in USD and the current value of your portfolio in USD.

The end goal of this script is to call the Bitstamp Websocket and HTTP APIs to get the current price of a cryptocurrency (in USD) as well as historical data and write the data to InfluxDB for visualization in Grafana.

The script currently has a hardcoded list of cryptocurrencies which can be updated to include any cryptocurrency that is supported by Bitstamp.

## Future Updates and Features

A future update will include the ability to add and remove cryptocurrencies from the list of cryptocurrencies that one may want to track. Additionally, the script will be updated to allow for dynamic entry of the portfolio holdings and the ability to add and remove holdings from the portfolio.

This may also be extended to include the ability to pull data from wallets and exchanges to get the current value of the portfolio without the need to manually enter the holdings.

## Installation

To install the required packages, run the following command:

```bash
pip install -r requirements.txt
```

## Usage

Recommended usage it to run the script in a systemd service or via Alpine Linux's OpenRC. This will allow the script to run in the background and automatically restart if it crashes.

To run the script, use the following command:

```bash
python3 main.py
```
