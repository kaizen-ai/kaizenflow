"""
Import as:

import defi.dao_swap.twap_vwap_adapter as ddstvwad
"""

import json

import numpy as np
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# TODO(Dan): Replace with a company API. Now a dummy is used.
_API_KEY = "bef8f874d2ad2e08d1e0eb91bad9c53399b8ca55016ba01101f4eeb855abf70f"


def _get_price_volume_data():
    """
    Query price and volume data from the CryptoCompare API.
    """
    # Get parameters from the Chainlink node request.
    symbol = request.json["symbol"]
    start_time = request.json["start_time"]
    end_time = request.json["end_time"]
    request.json["time_interval"]
    # Query the CryptoCompare API for price data within the specified time range.
    response = requests.get(
        f"https://min-api.cryptocompare.com/data/v2/histohour?fsym={symbol}&tsym=USD&limit=2000&aggregate=1&toTs={end_time}&api_key={_API_KEY}"
    )
    price_data = json.loads(response.text)["Data"]["Data"]
    # Get price and volume data.
    prices = [
        float(entry["close"])
        for entry in price_data
        if entry["time"] >= start_time
    ]
    volumes = [
        float(entry["volumefrom"])
        for entry in price_data
        if entry["time"] >= start_time
    ]
    return prices, volumes


@app.route("/get_twap", methods=["POST"])
def get_twap():
    """
    Get TWAP for the Chainlink node.
    """
    prices, volumes = _get_price_volume_data()
    twap = np.average(prices, weights=volumes)
    twap = jsonify(
        {
            "jobRunID": request.json["jobRunID"],
            "data": {"result": str(twap)},
        }
    )
    return twap


@app.route("/get_vwap", methods=["POST"])
def get_vwap():
    """
    Get VWAP for the Chainlink node.
    """
    prices, volumes = _get_price_volume_data()
    vwap = np.sum(prices * volumes) / np.sum(volumes)
    vwap = jsonify(
        {"jobRunID": request.json["jobRunID"], "data": {"result": str(vwap)}},
    )
    return vwap


if __name__ == "__main__":
    app.run(debug=True)
