FROM python:3.13-slim

# Docker Compose builds this image by cloning the GitHub repo and using it
# as the build context, so all files (app.py etc.) are already present here.

WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    flask \
    tidalapi \
    pandas \
    requests \
    pyopenssl \
    cryptography

# Copy app from build context (= the cloned repo)
COPY . .

# /data is the persistent volume (token, certs, cache)
VOLUME ["/data"]

ENV FLASK_PORT=6001 \
    COUNTDOWN_SEC=3 \
    CACHE_DIR=/data/cache \
    TOKEN_FILE=/data/tidal_token.json

EXPOSE 6001

CMD ["python", "app.py"]