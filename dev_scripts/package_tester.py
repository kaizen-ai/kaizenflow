#!/usr/bin/env python

import sys
print((sys.version))

try:
    import numpy
    print(('numpy', numpy.__version__))
except ImportError as e:
    print('Can\'t import numpy, check packages.')

try:
    import pandas
    print(('pandas', pandas.__version__))
except ImportError as e:
    print('Can\'t import pandas, check packages.')

if False:
    try:
        import pyarrow
        print(('pyarrrow', pyarrow.__version__))
    except ImportError as e:
        print('Can\'t import pyarrow, check packages.')

    try:
        import joblib
        print(('joblib', joblib.__version__))
    except ImportError as e:
        print('Can\'t import joblib, check packages.')
