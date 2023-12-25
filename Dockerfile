FROM python:3.6
MAINTAINER Wenhui Zhou

COPY ./requirements.txt ./
RUN pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY . .

CMD ["gunicorn", "controller_server:app", "-c", "./gunicorn.conf.py"]