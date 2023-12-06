# How to extract data from Signal

- An example notebook is at
  `marketing/notebooks/SorrTask612_Get_information_from_Signal.ipynb`.

## Dataflow

- Input: A url from Signal investors list.
- Output: A pandas dataframe with investors' `First Name`, `Last Name` and
  `Company Name`.

## Usage

- Select a list in https://signal.nfx.com/investor-lists/ and open its page,
  copy the url of the list to the notebook. E.g.:
  `baseurl = https://signal.nfx.com/investor-lists/top-fintech-seed-investors`.
- You need to first determine the range of data to be extracted in this run by
  specifying the start index and the length of the data.
  - This is because the page is only loading a few items for one click on the
    loading button and the total length of data is unknown. We don't want the
    code to run forever.
- In any notebook, run the following script to get a pandas dataframe
  representing the specified data range of the list:

  ```python
  import marketing.signal as mrksign

  baseurl = <url_to_the_list>
  start_idx = <first_item_needed>
  length = <length_of_items_needed>
  signal_dataframe = mrksign.extract_investors_from_signal_url(baseurl, start_idx, length)
  ```
