FROM python:3.7
RUN mkdir /app
ADD ./twap_vwap_adapter.py /app
RUN pip install flask flask_restful numpy requests
CMD python /app/twap_vwap_adapter.py


