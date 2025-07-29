FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    wget unzip curl chromium chromium-driver \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "bot.py"]
