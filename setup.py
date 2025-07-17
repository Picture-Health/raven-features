from setuptools import setup, find_packages

setup(
    name="px_code",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "clearml",
        "boto3",
        "pandas",
        "numpy",
        "python-dotenv",
        "gitpython"
    ],
    python_requires=">=3.10",
)

