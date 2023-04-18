from serpapi import GoogleSearch


def fetch_data(topic):
    params = {
        "engine": "google_trends",
        "q": topic,
        "data_type": "TIMESERIES",
        "date": "all",
        "api_key": "a6089e527a003d73adcb30646872ead71c83df40135fb2dd19848fa1c1d12644"
    }

    search = GoogleSearch(params)
    results = search.get_dict()
    interest_over_time = results["interest_over_time"]

    return interest_over_time
