FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    locales \
    wget unzip curl chromium chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Генерируем локаль en_US.UTF-8
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "bot.py"]
