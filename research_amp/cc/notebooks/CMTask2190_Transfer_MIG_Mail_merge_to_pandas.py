# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
import pandas as pd

# %% [markdown]
# # Load the data

# %%
# Monster Investment Gsheet.
mig = pd.read_csv("Monster Investor Gsheet - Firms.csv")
# Unify the absense of e-mails.
mig["Submit by email"] = mig["Submit by email"].fillna("not available")
mig.head(3)

# %%
# Reach e-mails.
iterations = [
    "Mail_merge - Mail Merge - 2021-06-18.csv",
    "Mail_merge - Mail Merge - 2021-06-20.csv",
    "Mail_merge - Mail Merge - 2021-06-20-bis.csv",
]

mail_merge = []
for file in iterations:
    df_tmp = pd.read_csv(file).iloc[: , :4]
    df_tmp.columns = [
        "Email",
        "Name",
        "Company",
        "Consensus"
    ]
    mail_merge.append(df_tmp)
mail_merge = pd.concat(mail_merge)
mail_merge.tail(3)

# %% [markdown]
# # Select the contacts that haven't been reached yet

# %%
# Convert MIG to the e-mail reach format.
mig_emails = mig[mig["Submit by email"]!="not available"][["Submit by email", "Name"]]
mig_emails.shape

# %%
# Extract e-mails that were previously used in campaign.
mail_merge_emails = list(mail_merge["Email"])
# Filter by those names for new unique e-mails.
new_iteration = mig_emails[~mig_emails["Submit by email"].isin(mail_merge_emails)]
display(new_iteration)

# %%
new_iteration.to_csv("new_iteration.csv")
