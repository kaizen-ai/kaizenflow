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

# %% [markdown]
# # Search name

# %%
search_name = "sn_search5"

# %% [markdown]
# # Load data

# %%
gsheet_name = f"{search_name}.step1.search_export"
spread = gspread_pandas.Spread(gsheet_name)
df = spread.sheet_to_df(index=None)
print(df.shape)
df.head()

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
# Filter by location.
# The location you want to keep.
location = "Maryland"
prev_len = len(df)
df = df[df["location"].apply(lambda x: location in x)].reset_index(drop=True)
print(
    f"Dropped {prev_len - len(df)} rows ({round((prev_len - len(df))*100/prev_len, 2)}%)"
)
df.head()

# %%
# Make sure profileUrl in the first column.
cols = df.columns.tolist()
if cols[0] != 'profileUrl':
    cols.remove('profileUrl')  # Remove the "profileURL" from its current position.
    cols = ['profileUrl'] + cols  # Add "profileURL" at the first column.
    df = df[cols] 
df.head()

# %%
print(df.shape)

# %% [markdown]
# # Save filtered data to gsheet

# %%
# A Google sheet with this name should already exist on the drive.
gsheet_name = f"{search_name}.step2.search_export_filtered"
spread2 = gspread_pandas.Spread(
    gsheet_name,
    sheet="Sheet1",
    create_sheet=True,
)
spread2.df_to_sheet(df, index=False)

# %%
