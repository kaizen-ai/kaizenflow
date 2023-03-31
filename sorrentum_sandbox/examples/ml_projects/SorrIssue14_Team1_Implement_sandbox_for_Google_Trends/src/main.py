import utilities

if __name__ == "__main__":
    # raw_data = fetch.fetch_data("Summer")
    # print(raw_data)

    clean_data = utilities.extract_data_from_json("data/data.json", True)
    clean_data["Topic"] = ["summer"]*len(clean_data)

    clean_data = clean_data[['Topic', 'Time', 'Frequency']]
    print("data cleaned")

    clean_data.to_csv("data/cleaned_data.csv", index=False)
    print("Fetched Data: \n", clean_data)

