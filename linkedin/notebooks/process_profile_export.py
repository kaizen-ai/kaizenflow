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

# %% [markdown]
# # Imports

# %%
import gspread_pandas
import pandas as pd

# %% [markdown]
# # Load data

# %%
spread = gspread_pandas.Spread("search_retired1.profile_export.gsheet")
df = spread.sheet_to_df(index=None)
print(df.shape)
df.head()

# %%
df.columns

# %%
for col in df.columns:
    print(col)
    print(df.iloc[0][col])
    print()

# %% [markdown]
# # Filter data

# %%
# Filter by keywords.
keywords = ["volunteer", "adjunct", "consult"]
prev_len = len(df)
dfs_filtered = []
for col in [
    "headline",
    "jobTitle",
    "jobDescription",
    "jobTitle2",
    "jobDescription2",
    "allSkills",
]:
    dfs_filtered.append(
        df[df[col].apply(lambda x: any(kw in x.lower() for kw in keywords))]
    )
df = pd.concat(dfs_filtered).drop_duplicates().reset_index(drop=True)
print(
    f"Dropped {prev_len - len(df)} rows ({round((prev_len - len(df))*100/prev_len, 2)}%)"
)

# %%
print(df.shape)
df

# %% [markdown]
# # Save filtered data to gsheet

# %%
# A Google sheet with this name should already exist on the drive.
spread2 = gspread_pandas.Spread(
    "search_retired1.profile_export.filtered.gsheet",
    sheet="Sheet1",
    create_sheet=True,
)
spread2.df_to_sheet(df, index=False)

# %%
