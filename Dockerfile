FROM python:3.12-slim-bookworm

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED 1

# Create and change to the app directory.
WORKDIR /usr/src/app

# Copy application dependency manifests to the container image.
# Copying this separately prevents re-running pip install on every code change.
COPY requirements.txt ./

# Install dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Copy local code to the container image.
COPY . ./

ENTRYPOINT python bot.py
