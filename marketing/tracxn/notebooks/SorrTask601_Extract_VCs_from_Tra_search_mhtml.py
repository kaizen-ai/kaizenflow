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

# %% [markdown]
# # Workflow before using this library:
#
# 1. Go to a Tra VCs search result page
# 2. Use the browser's `Save As` button to download the webpage as a `Web page, single file`. Or use any other download method that can fulfill the requirement in step 3.
# 3. If you see the downloaded file format is `.mht` or `.mhtml`, you can process forward. Otherwise you won't be able to bypass the check layer from the website.
# 4. Call `get_VCs_from_mhtml` method with the `.mhtml` file path.
# 5. Save the returned dataframe to whatever format preferred.

# %% run_control={"marked": true}
import marketing.tracxn as mrktra

# %% [markdown]
# # Sample usage of the function.

# %%
# !ls ../..

# %% run_control={"marked": true}
# Source data file path.
vc_mhtml_path = "../../Tracxn_VCs_AI_SeriesA_Seed.mht"
vc_df = mrktra.get_VCs_from_mhtml(vc_mhtml_path)
vc_df

# %%
vc_df.iloc[0]

# %%
# Destination result file path.
vc_csv_save_path = "./Investors_VC_Tra.csv"
# Get Dataframe of VCs from HTML page.
vc_df.to_csv(vc_csv_save_path, sep=",", index=False)

# %%
# # !sudo /bin/bash -c "(source /venv/bin/activate; pip install csvkit)"

# %%
# # !csvlook $vc_csv_save_path
