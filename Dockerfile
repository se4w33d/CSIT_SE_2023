FROM python:3.9.17-slim

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY /src /app/src

WORKDIR /app/src

CMD ["uvicorn", "server:app", "--host", "0.0.0.0"]

EXPOSE 8080