FROM python:3.10-slim

RUN apt-get update
RUN apt-get -y install curl libpq-dev gcc unixodbc

# DEPENDECES FOR DOWNLOAD ODBC DRIVER
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update

# INSTALL ODBC DRIVER
RUN ACCEPT_EULA=Y apt-get install msodbcsql17 --assume-yes

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
