FROM python:3.9-slim as compiler
ENV PYTHONUNBUFFERED 1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY faas-sim-evaluation/requirements-docker.txt .
RUN pip install -r requirements-docker.txt



FROM python:3.9-slim as build-image
COPY --from=compiler /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY faas faas
COPY galileo-faas galileo-faas
COPY faas-sim faas-sim

RUN pip install -e faas
RUN pip install -e galileo-faas
RUN pip install -e faas-sim

COPY faas-optimizations faas-optimizations
RUN pip install -e faas-optimizations

COPY faas-sim-evaluation/evaluation ./evaluation
