FROM python:3.12-slim
LABEL maintainer="edenceHealth NV <info@edence.health>"

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# these 2 variables are expected by the app code
ARG COMMIT_SHA=dev
ARG GITHUB_TAG=dev
ENV COMMIT_SHA="${COMMIT_SHA}" GITHUB_TAG="${GITHUB_TAG}"

COPY src/etl /app/etl/
WORKDIR /work
ENV PYTHONPATH="/app"

ENTRYPOINT [ "python", "-m", "etl" ]
