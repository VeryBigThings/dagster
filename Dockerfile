FROM python:3.10-slim

RUN apt-get update
RUN apt-get -y install curl lsb-release libpq-dev gcc unixodbc

RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# All packages are hard-pinned to `dagster`, so setting the version on just `DAGSTER` will ensure
# compatible versions.
RUN pip install -U uv
RUN python -m uv pip install \
    dagster==1.8.12 \
    dagster-postgres \
    dagster-aws \
    dagster-k8s \
    dagster-celery[flower,redis,kubernetes] \
    dagster-celery-k8s \
    dagstermill \
    papermill  \
    matplotlib \
    seaborn \
    scikit-learn \
    pandas \
    pyodbc \
    psycopg2

# Get example pipelines
COPY ./ /
