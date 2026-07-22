# Dockerfile do monitor web (Python, apenas biblioteca padrao).
FROM python:3.12-slim
WORKDIR /app
COPY monitor/monitor.py monitor/dashboard.html ./
ENV DOCKER_MODE=1
EXPOSE 8080
CMD ["python", "monitor.py"]
