FROM python:3.8.13

RUN apt-get update
RUN apt-get install -y librdkafka-dev

RUN pip install sentry-arroyo

COPY . /app
