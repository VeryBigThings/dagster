FROM python:3.10-slim

RUN apt-get update
RUN apt-get -y install curl gnupg2 lsb-release libpq-dev gcc unixodbc

RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
RUN curl https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# All packages are hard-pinned to `dagster`, so setting the version on just `DAGSTER` will ensure
# compatible versions.
RUN pip install -U uv
RUN python -m uv pip install \
    dagster==1.8.12 \
    dagster-postgres==0.24.12 \
    dagster-aws==0.24.12 \
    dagster-k8s==0.24.12 \
    dagster-celery[flower,redis,kubernetes]==0.24.12 \
    dagster-celery-k8s==0.24.12 \
    dagstermill==0.24.12 \
    dagster-webserver[notebook]==1.8.12 \
    nbconvert \
    notebook \
    papermill  \
    matplotlib \
    seaborn \
    scikit-learn \
    pandas \
    pyodbc \
    psycopg2 \
    pymssql

# Get example pipelines
COPY ./ /
