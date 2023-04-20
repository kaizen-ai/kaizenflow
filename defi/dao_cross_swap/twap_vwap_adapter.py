"""
Import as:

import defi.dao_swap.twap_vwap_adapter as ddstvwad
"""

import os
import json
from typing import Any, Dict, List, Tuple, Callable

import numpy as np
import requests
from flask import Flask, jsonify, request, abort
from functools import wraps

app = Flask(__name__)

API_KEY = os.environ.get("API_KEY")
# Specify FLASK_DEBUG_MODE=0 To turn off debug mode.
DEBUG_MODE = os.environ.get("FLASK_DEBUG_MODE", "1") == "1"


def require_api_key(func: Callable) -> Callable:
    """
    Perform authorization of a request.
    """
    @wraps(func)
    def check_api_key(*args, **kwargs):
        api_key = request.headers.get("X-API-KEY")
        if not api_key or api_key != API_KEY:
            abort(401, "Unauthorized: Invalid API key")
        return func(*args, **kwargs)
    
    return check_api_key


def _get_price_volume_data() -> Tuple[List[int], List[float]]:
    """
    Query price and volume data from the CoinGecko API.
    """
    # Get parameters from the Chainlink node request.
    symbol = request.json["symbol"]
    start_time = request.json["start_time"]
    end_time = request.json["end_time"]
    # Query the CoinGecko API for price data within the specified time range.
    response = requests.get(
        f"https://api.coingecko.com/api/v3/coins/{symbol}/market_chart/range?vs_currency=eth&from={start_time}&to={end_time}"
    )
    price_data = json.loads(response.text)["prices"]
    # Get price and volume data.
    prices = [float(entry[0]) for entry in price_data]
    volumes = [float(entry[1]) for entry in price_data]
    # Convert prices to WEI.
    eth_to_wei = 10**18
    prices = [int(price * eth_to_wei) for price in prices]
    return prices, volumes


@app.route("/get_twap", methods=["POST"])
@require_api_key
def get_twap() -> Dict[str, Any]:
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
@require_api_key
def get_vwap() -> Dict[str, Any]:
    """
    Get VWAP for the Chainlink node.
    """
    prices, volumes = _get_price_volume_data()
    vwap = np.sum(prices * volumes) / np.sum(volumes)
    vwap = jsonify(
        {
            "jobRunID": request.json["jobRunID"],
            "data": {"result": str(vwap)},
        }
    )
    return vwap


if __name__ == "__main__":
    app.run(debug=DEBUG_MODE)
