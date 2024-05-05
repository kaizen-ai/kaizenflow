**To run code**


docker-compose up -d

docker exec -it my_jupyter_container /bin/bash (*To access the container terminal*)


**Once inside the container terminal, convert the notebook to a script**


jupyter nbconvert --to script main_app.ipynb
(*this command will sometimes need to be run multiple times*)

mv main_app.txt main_app.py


**Finally, you can run the script**

python main_app.py


**Or, to view the notebook**
Navigate to http://localhost:8888/ in your browser