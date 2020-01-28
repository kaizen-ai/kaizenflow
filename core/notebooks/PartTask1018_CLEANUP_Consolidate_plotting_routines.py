# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python [conda env:.conda-p1_develop] *
#     language: python
#     name: conda-env-.conda-p1_develop-py
# ---

# %%
# %load_ext autoreload
# %autoreload 2
import core.plotting as plot
import seaborn as sns
import pandas as pd
import random
from IPython.display import display

# %%
df = pd.read_csv('/data/tmp_ravenpack/RavenPack_Analytics_taxonomy_1.0.csv')
df.shape

# %%
plot.plot_categories_count(df, "TOPIC", title = "Topics in the taxonomy", label="row")

# %%
df_topics_groups = df[["TOPIC","GROUP"]].groupby(["TOPIC","GROUP"]).count().reset_index()
plot.plot_categories_count(df_topics_groups, "TOPIC", title = "Number of groups in topcis", label="group")

# %%
df_business_topic = df[df["TOPIC"] == "business"]
plot.plot_categories_count(df_business_topic, "GROUP", title = "Number of types in differnet 'business' groups", label="row")

# %%
df['commodity'] = df['VALID_ENTITY_TYPES'].apply(lambda x: 'COMMODITY' in x)
df_commodity = df[df['commodity'] == True]
print(df_commodity.shape)
df_commodity.head(5)

# %%
plot.plot_categories_count(df_commodity, "TOPIC", title = "Topics in the taxonomy", label="row")

# %%
plot.plot_categories_count(df_commodity, "GROUP", title = "Groups in the taxonomy", label="row")

# %%
predictions = [random.randint(0, 1)  for x in range(0,11)]
y_test = [random.randint(0, 1)  for x in range(0,11)]

# %%
predictions

# %%
y_test

# %%
a, b = plot.plot_confusion_heatmap(y_test, predictions, return_results = True)
display(a, b)

# %%
plot.plot_confusion_heatmap(y_test, predictions, return_results = False)
