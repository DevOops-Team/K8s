# 잡 투입기 이미지: 실험마다 같은 코드·의존성으로 돌게 (시작할 때 pip install 하지 않음)
# 빌드: docker build -t submitter scheduling/submitter
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY submit.py .
ENV PYTHONUNBUFFERED=1 HOME=/tmp
USER 65532
ENTRYPOINT ["python", "/app/submit.py"]
