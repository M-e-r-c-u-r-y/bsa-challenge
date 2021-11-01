FROM python:3.9.5-slim-buster

ENV PYTHONUNBUFFERED=1

WORKDIR /home/kaggle-job

RUN apt-get update
RUN apt-get install build-essential libffi-dev libssl-dev rustc curl -y --no-install-recommends
RUN rm -rf /var/lib/apt/lists/*

ADD ./requirements.txt /home/kaggle-job
RUN pip install -r requirements.txt --no-cache-dir --compile

RUN apt-get purge build-essential libffi-dev libssl-dev rustc -y
RUN apt-get autoremove -y
RUN apt-get autoclean
RUN apt-get clean

ADD ./* /home/kaggle-job

EXPOSE 8000

ENV PORT 8000

CMD ["uvicorn", "main:app", "--reload", "--workers", "1", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]
