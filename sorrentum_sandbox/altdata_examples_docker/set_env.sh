set -o vi
jupyter nbextension enable vim_binding/vim_binding
cd /data
export PYTHONPATH=$PYTHONPATH:/data
python3 -c "import helpers.hdbg"
