FROM netortech/scrap-worker:latest
USER root

COPY ./requirements-test.txt /tmp/requirements-test.txt

RUN echo "Python Version:" && \
    python --version && \
    echo "Installing pip packages for tests..." && \
    python -m pip install -r /tmp/requirements-test.txt

COPY ./docker/scripts/lint-and-test.sh /tmp/lint-and-test.sh

ENTRYPOINT [ "/tmp/lint-and-test.sh" ]
