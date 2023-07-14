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
# # Load data

# %%
spread = gspread_pandas.Spread("search_retired1.search_export.gsheet")
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

# %%
# Filter by location.
prev_len = len(df)
df = df[df["location"].apply(lambda x: "MD" in x)].reset_index(drop=True)
print(
    f"Dropped {prev_len - len(df)} rows ({round((prev_len - len(df))*100/prev_len, 2)}%)"
)

# %%
print(df.shape)
df

# %% [markdown]
# # Save filtered data to gsheet

# %%
spread2 = gspread_pandas.Spread(
    "search_retired1.search_export.filtered.gsheet",
    sheet="Sheet1",
    create_sheet=True,
)
spread2.df_to_sheet(df, index=False)

# %%
