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

# %%
import marketing.tra.extract_people_from_Tra_company_mhtml as mtepftcmh

# %% [markdown]
# # Workflow before using this library:
#
# 1. Go to a Tra company's people page ("\<company_page\>/people/currentteam")
# 2. Use the browser's `Download` or `Save As` button to download the webpage as a `Web page, single file`. Or use any other download method that can fulfill the requirement in step 3.
# 3. If you see the downloaded file format is `.mht` or `.mhtml`, you can process forward. Otherwise you won't be able to bypass the check layer from the website.
# 4. Use the `.mhtml` file path as the parameter to `get_employees_from_html` method, it will return a dataframe. Save it to whatever format preferred.

# %% [markdown]
# # Sample usage of the library.

# %%
# Source data file path.
employee_mhtml_path = "../data/Sequoia Capital _ Tra.mhtml"
# Destination result file path.
employee_csv_save_path = "../result_csv/Sequoia Capital _ Tra.csv"
# Get Dataframe of employees from HTML page.
employee_df = mtepftcmh.get_employees_from_mhtml(employee_mhtml_path)
employee_df.to_csv(employee_csv_save_path, sep=",", index=False)
employee_df

# %%
