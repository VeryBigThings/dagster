from setuptools import find_packages, setup

setup(
    name="etl_assets",
    packages=find_packages(exclude=["etl_assets"]),
    install_requires=[
        "dagster",
        "dagstermill",
        "pandas",
        "dagster-aws",
        "matplotlib",
        "seaborn",
        "scikit-learn",
        "pyodbc",
        "psycopg2",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
