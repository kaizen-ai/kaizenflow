# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# !sudo /bin/bash -c "(source /venv/bin/activate; pip install gspread_pandas)"

# %%
import gspread_pandas
import pandas as pd


# %%
def convert_spreadsheet_to_df(spread_name: str) -> pd.DataFrame:
    spread = gspread_pandas.Spread(spread_name)
    df = spread.sheet_to_df(index=None)
    return df


# %% [markdown]
# # Search Export Difference

# %%
LI_Search1 = 'search_retired1.search_export.gsheet'
SN_Search5 = 'sn_search5.search_export.gsheet'

# %%
LI_Search_df = convert_spreadsheet_to_df(LI_Search1)
LI_Search_df.iloc[1].to_dict()

# %%
SN_Search_df = convert_spreadsheet_to_df(SN_Search5)
print(SN_Search_df)

# %% heading_collapsed=true
LI_Search_df.columns

# %%
SN_Search_df.columns

# %%
LI_Search_list = LI_Search_df.columns.tolist()
SN_Search_list = SN_Search_df.columns.tolist()

# %%
# Create a combined set of all unique elements from both lists
LI_Search_list = LI_Search_df.columns.tolist()
SN_Search_list = SN_Search_df.columns.tolist()
all_elements = list(set( LI_Search_list + SN_Search_list ))

# For each element, check if it is in list1 or list2
presence_in_lists = [(element, int(element in LI_Search_list), int(element in SN_Search_list)) for element in all_elements]

# Create a dataframe from this data
df_presence = pd.DataFrame(presence_in_lists, columns=['Field', 'LinkedIn', 'Sales Navigator'])

df_presence

# %%
df_presence['order'] = df_presence['Field'].apply(lambda x: LI_Search_list.index(x) if x in LI_Search_list else SN_Search_list.index(x))
df_sorted_by_original = df_presence.sort_values(by=['LinkedIn', 'order'], ascending=[False, True])

# Drop the 'order' column
df_sorted_by_original = df_sorted_by_original.drop('order', axis=1)
df_sorted_by_original

# %%
# Add an example to each field.
# Initialize a dictionary to store example values
LI_example = LI_Search_df.iloc[1].to_dict()
SN_example = SN_Search_df.iloc[3].to_dict()
example_values = {}

# Fill in example values from list1_example and list2_example
for element in df_sorted_by_original['Field']:
    if element in LI_example:
        example_values[element] = LI_example[element]
    elif element in SN_example:
        example_values[element] = '[SN] ' + SN_example[element]
    else:
        example_values[element] = ''

# Add the example values to the dataframe
df_sorted_by_original['Example'] = df_sorted_by_original['Field'].map(example_values)

df_sorted_by_original

# %% [markdown]
# # Profile Export Difference

# %%
LI_Profile1 = 'search_retired1.profile_export.gsheet'
SN_Profile5 = 'sn_search5.profile_export.gsheet'
