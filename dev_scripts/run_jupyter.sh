if [[ -z $1 ]]; then
  ENV="jupyter"
else
  ENV=$1
fi;
echo "ENV=$ENV"

source dev_scripts/setenv.sh $ENV

conda info --envs

if [[ -z $2 ]]; then
  PORT=9999
else
  PORT=$2
fi;
echo "PORT=$PORT"

jupyter notebook --ip=* --browser="chrome" . --port=$PORT
