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
import re

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
    df_tmp = pd.read_csv(file).iloc[:, :4]
    df_tmp.columns = ["Email", "Name", "Company", "Consensus"]
    mail_merge.append(df_tmp)
mail_merge = pd.concat(mail_merge)
mail_merge.tail(3)

# %% [markdown]
# # Select the contacts that haven't been reached yet

# %% [markdown]
# ## Drop the contacts that are already in the pipeline

# %%
# Convert MIG to the e-mail reach format.
mig_emails = mig[mig["Submit by email"] != "not available"][
    ["Submit by email", "Name"]
]
mig_emails.shape

# %%
# Extract e-mails that were previously used in campaign.
mail_merge_emails = list(mail_merge["Email"])
# Filter by those names for new unique e-mails.
new_iteration = mig_emails[~mig_emails["Submit by email"].isin(mail_merge_emails)]
display(new_iteration)


# %% [markdown]
# ## Sanity check

# %%
def check_email_format(email):
    # Regular expression for validating an Email.
    regex_email = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    # Sanity check.
    if not (re.fullmatch(regex_email, email)):
        email = None
    return email


# %%
new_iteration_cleaned = new_iteration.copy()
# Replace "bad" emails with NaN.
new_iteration_cleaned["Submit by email"] = new_iteration_cleaned[
    "Submit by email"
].apply(lambda x: check_email_format(x))
# Save "bad" emails in a separate DataFrame for manual check.
bad_emails_list = list(
    new_iteration_cleaned[new_iteration_cleaned["Submit by email"].isna()]["Name"]
)
bad_emails = new_iteration[new_iteration["Name"].isin(bad_emails_list)]
bad_emails

# %%
# Get rid of NaNs in "clean" contacts.
new_iteration_cleaned = new_iteration_cleaned[
    new_iteration_cleaned["Submit by email"].notna()
]
new_iteration_cleaned

# %% [markdown]
# ## Save the file

# %%
# new_iteration_cleaned.to_csv("new_iteration.csv")
