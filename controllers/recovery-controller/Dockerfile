FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY controller.py workflow.py adapters.py runtime.py jobsets.py leadership.py /app/
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["python", "-u", "/app/controller.py"]
