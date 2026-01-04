FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY data /app/data

RUN mkdir -p /app/output /app/profiling /app/docs

CMD ["bash", "-lc", "python -u src/main.py && python -u src/make_data_dictionary.py"]
