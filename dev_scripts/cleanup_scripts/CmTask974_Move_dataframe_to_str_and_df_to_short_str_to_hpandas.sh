#!/bin/bash -xe

# Move dataframe_to_str from hprint to hpandas.
./dev_scripts/replace_text.py \
  --old "hprint.dataframe_to_str" \
  --new "hpandas.dataframe_to_str" \
  --ext "py"

# Move df_to_short_str from hprint to hpandas.
./dev_scripts/replace_text.py \
  --old "hprint.df_to_short_str" \
  --new "hpandas.df_to_short_str" \
  --ext "py"

# Add potential missing imports.
./dev_scripts/replace_text.py \
  --old "import helpers.hprint as hprint" \
  --new "import helpers.hpandas as hpandas; import helpers.hprint as hprint" \
  --ext "py"

# Remove unused imports.
i lint --files "$(find . -name '*.py')" --phases="amp_isort autoflake"
