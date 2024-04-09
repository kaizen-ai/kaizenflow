FROM tiangolo/uwsgi-nginx-flask:python3.8-alpine
RUN apk --update add bash nano
ENV STATIC_URL /static
ENV STATIC_PATH /Documents/umd_data605/sorrentum/sorrentum_sandbox/spring2024/Trask_basic_rate_limiting_middleware_with_Redis/app/static
COPY ./requirements.txt /Documents/umd_data605/sorrentum/sorrentum_sandbox/spring2024/Trask_basic_rate_limiting_middleware_with_Redis/requirements.txt
RUN pip install -r /Documents/umd_data605/sorrentum/sorrentum_sandbox/spring2024/Trask_basic_rate_limiting_middleware_with_Redis/requirements.txt
