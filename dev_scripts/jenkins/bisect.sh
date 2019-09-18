# egrep "FAILED|git checkout -f" /data/jenkins/jobs/.../builds/169/log

git bisect reset
git bisect start
git bisect bad d15b3d69a4cbb6c3198438f6a824eec270b80b57
git bisect good 6005d4c12591d6460c926c6d9245e5af7c58cefa

git bisect run pytest TEST
