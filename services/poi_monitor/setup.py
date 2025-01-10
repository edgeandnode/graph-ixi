from setuptools import setup, find_packages

setup(
    name="poi_monitor",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "requests",
        "python-dotenv",
        "psycopg2-binary",
        "prometheus-client",
        "pyyaml",
        "python-json-logger",
    ],
) 