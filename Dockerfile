FROM python:3.10

WORKDIR /app
COPY . .
RUN mkdir -p common
RUN mkdir -p log

COPY tools.py common
COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "upppi.py"]