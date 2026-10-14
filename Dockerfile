FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DJQ_DB=/app/var/queue.db DJQ_OUTPUT_DIR=/app/var/output
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --create-home queue
COPY api api
COPY common common
COPY server server
COPY worker worker
RUN mkdir -p /app/var/output && chown -R queue:queue /app/var
USER queue
EXPOSE 5010 5555
CMD ["python", "-m", "server", "--host", "0.0.0.0"]
