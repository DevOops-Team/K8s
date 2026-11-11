FROM python:3.12-slim
WORKDIR /app
COPY common/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/*.py .
COPY relocator/*.py ./relocator/
ENV PYTHONPATH=/app
USER 65532
CMD ["python", "relocator/relocator.py"]
