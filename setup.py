from setuptools import setup, find_packages

setup(
    name="breadmanager",
    version="0.1",
    packages=find_packages(exclude=["scripts"]),
    install_requires=[
        "boto3>=1.34.157",
        "ib_async>=1.0.3",
        "pandas>=2.2.2",
        "psycopg2-binary>=2.9.9",
        "python-dotenv>=1.0.1",
    ],
)
