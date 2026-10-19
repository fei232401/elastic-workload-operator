FROM python:3.12-slim
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG TORCH_FIND_LINKS=https://download.pytorch.org/whl/cpu/torch/
RUN pip install --no-cache-dir --index-url "${PIP_INDEX_URL}" --find-links "${TORCH_FIND_LINKS}" "torch==2.11.0+cpu"
WORKDIR /app
COPY train.py entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh && useradd --uid 10001 trainer
USER 10001
ENTRYPOINT ["/app/entrypoint.sh"]
