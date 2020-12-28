FROM python:3.7-buster
USER root

COPY ./requirements.txt /tmp/requirements.txt

RUN echo "Python Version:" && \
    python --version && \
    echo "Installing pip packages..." && \
    python -m pip install -r /tmp/requirements.txt && \
    echo "Setting up non-root user and www group" && \
    groupadd -g 80 www || true && \
    useradd -u 8080 -G www scrap-worker

COPY ./src /app

USER scrap-worker
WORKDIR /app

ENTRYPOINT [ "python", "manage.py", "start" ]
