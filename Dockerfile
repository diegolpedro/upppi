FROM python:3.10

WORKDIR /app
COPY upppi/. .
RUN mkdir -p common
RUN mkdir -p log

COPY common/. common
COPY upppi/requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "upppi.py"]