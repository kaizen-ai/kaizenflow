ghi show $* | head -1
echo "https://github.com/ParticleDev/commodity_research/issues/$*"

echo
echo "Files in the repo:"
regex="Task${*}*\.*py*"
echo "regex=$regex"
find . -name $regex | grep -v ipynb_checkpoint
