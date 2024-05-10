FROM python:3.7

WORKDIR /app

RUN pip install --no-cache-dir scikit-learn pandas matplotlib seaborn

COPY 605_pro.py .

VOLUME ["/intput","/output"]

CMD ["python","605_pro.py"]
