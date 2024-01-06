FROM python:3.6
MAINTAINER Wenhui Zhou

COPY ./requirements.txt ./
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

WORKDIR /app
COPY client.py controller_server.py gunicorn.conf.py log.py utils.py /app/

CMD ["gunicorn", "controller_server:app", "-c", "./gunicorn.conf.py"]