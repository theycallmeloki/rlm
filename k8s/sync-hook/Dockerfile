FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir flask

COPY sync_hook.py .

EXPOSE 8080

CMD ["python", "sync_hook.py"]
