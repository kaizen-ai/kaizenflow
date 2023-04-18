import json
import pandas as pd


def extract_data_from_json(json_file, read_file_from_path):
    fetched_json_file = None

    if read_file_from_path:
        with open(json_file, 'r') as file:
            fetched_json_file = json.load(file)

    columns = ["Time", "Frequency"]
    cleaned_data = []
    raw_data = fetched_json_file["timeline_data"]

    for record in raw_data:
        cleaned_data.append(
            [
                record["date"].replace("\u2009", " "),
                record["values"][0]["extracted_value"]
            ]
        )

    df = pd.DataFrame(cleaned_data, columns=columns)

    return df


def perform_checks(df):
    print("Performing checks...")
    if True in df["Frequency"] > 100:
        return False

    elif True in df["Frequency"] < 0:
        return False

    print("Checks passed.")
    return True


if __name__ == "__main__":
    extract_data_from_json("../Files/data.json", True)