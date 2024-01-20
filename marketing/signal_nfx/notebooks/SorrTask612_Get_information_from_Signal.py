# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% run_control={"marked": true}
# TODO(Henry): Remove this when selenium and webdriver manager are added to the docker image.
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install --upgrade selenium webdriver-manager)"

# %%
import marketing.signal_nfx as mrksign

# %% run_control={"marked": true}
# Destination result file path.
investors_csv_save_path = "../result_csv/signal_fintech_investors.csv"
# Source data page URL.
baseurl = "https://signal.nfx.com/investor-lists/top-fintech-seed-investors"
# Specifying the range of data to be extracted.
start_idx = 0
length = 40
# Get Dataframe of investors from Signal page URL.
investors_df = mrksign.extract_investors_from_signal_url(baseurl, start_idx, length)
investors_df.to_csv(employee_csv_save_path, sep=",", index=False)
investors_df

# %%
