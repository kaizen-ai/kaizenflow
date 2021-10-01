"""
When this module is imported certain annoying warnings are disabled.

Import as:

import helpers.warnings_helpers as hwah
"""

_WARNING = "\033[33mWARNING\033[0m"
print(f"{_WARNING}: Disabling annoying warnings")

import warnings

# From https://docs.python.org/3/library/warnings.html

# TODO(gp): For some reason "once" doesn't work, so we ignore all of the warnings.
action = "ignore"

# /venv/lib/python3.8/site-packages/statsmodels/tsa/stattools.py:1910:
# InterpolationWarning: The test statistic is outside of the range of p-values
# available in the look-up table. The actual p-value is greater than the
# p-value returned.
from statsmodels.tools.sm_exceptions import InterpolationWarning

#warnings.simplefilter("ignore", category=InterpolationWarning)

# /venv/lib/python3.8/site-packages/statsmodels/tsa/stattools.py:1906:
# InterpolationWarning: The test statistic is outside of the range of p-values
# available in the look-up table. The actual p-value is smaller than the
# p-value returned.
warnings.filterwarnings(action, category=InterpolationWarning,
        module='.*statsmodels.*',
        lineno=1906,
        append=False)


warnings.filterwarnings(action, category=InterpolationWarning,
        module='.*statsmodels.*',
        lineno=1910,
        append=False)

# /venv/lib/python3.8/site-packages/ipykernel/ipkernel.py:283:
# DeprecationWarning: `should_run_async` will not call `transform_cell`
# automatically in the future. Please pass the result to `transformed_cell`
# argument and any exception that happen during thetransform in
# `preprocessing_exc_tuple` in IPython 7.17 and above.
#  and should_run_async(code)
warnings.filterwarnings(action, category=DeprecationWarning,
        module='.*ipykernel.*',
        lineno=283,
        append=False)
