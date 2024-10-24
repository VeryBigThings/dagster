FROM python:3.10-slim

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
    pandas

# Get example pipelines
COPY ./ /
