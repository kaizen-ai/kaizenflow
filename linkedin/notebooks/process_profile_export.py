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
# # Search name

# %%
search_name = "sn_search5"

# %% [markdown]
# # Load data

# %%
gsheet_name = f"{search_name}.step3.profile_export"
spread = gspread_pandas.Spread(gsheet_name)
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
# Drop errors.
prev_len = len(df)
df = df[df["error"] == ""].reset_index(drop=True)
df = df[[col for col in df.columns if col != "error"]]
print(
    f"Dropped {prev_len - len(df)} rows ({round((prev_len - len(df))*100/prev_len, 2)}%)"
)
df.head()

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
gsheet_name = f"{search_name}.step4.profile_export_filtered"
spread2 = gspread_pandas.Spread(
    gsheet_name,
    sheet="Sheet1",
    create_sheet=True,
)
spread2.df_to_sheet(df, index=False)

# %%
