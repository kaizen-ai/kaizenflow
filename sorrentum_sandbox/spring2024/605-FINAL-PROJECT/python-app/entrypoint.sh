#!/bin/bash

# Check the mode set in APP_MODE and start the appropriate application
if [ "$APP_MODE" = "jupyter" ]; then
    # Start Jupyter Notebook
    jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root
else
    # Start the Python application
    exec "$@"
fi
