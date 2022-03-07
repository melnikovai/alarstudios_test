FROM python:3.10.2-buster

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN mkdir /app
RUN pip install requests && pip install numpy && pip install tzwhere && pip install psycopg2-binary

ENV ALAR_USERNAME="test"
ENV ALAR_PASSWORD="123"
ENV PG_PASSWORD="postgres"

EXPOSE 5000 5432 80 22