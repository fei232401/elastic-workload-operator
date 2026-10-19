FROM python:3.12-slim
RUN pip install --no-cache-dir torch==2.11.0 --index-url https://download.pytorch.org/whl/cpu
WORKDIR /app
COPY train.py entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh && useradd --uid 10001 trainer
USER 10001
ENTRYPOINT ["/app/entrypoint.sh"]
