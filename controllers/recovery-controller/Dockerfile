FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py /app/main.py
COPY recovery/ /app/recovery/
COPY integrations/ /app/integrations/
COPY runtime/ /app/runtime/
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["python", "-u", "/app/main.py"]
