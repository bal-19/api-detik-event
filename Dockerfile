FROM python:3.12-slim

RUN mkdir -p /app

WORKDIR /app

COPY . /app

RUN pip install -r /app/requirements.txt

EXPOSE 80

CMD [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80" ]